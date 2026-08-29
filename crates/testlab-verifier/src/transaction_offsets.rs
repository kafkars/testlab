//! Transactional offset verification joins public group fences to independent broker truth.

use std::collections::BTreeMap;

use testlab_schema::{
    BrokerObservation, GroupProtocol, OperationId, Scenario, ScenarioAction,
    TransactionDisposition, TransactionalTransformAction, Violation,
};

use crate::consumer::{exact_record, sent_record};
use crate::index::{HistoryIndex, IndexedTransactionalTransform};
use crate::support::violation;

pub(crate) fn verify(
    scenario: &Scenario,
    action: &TransactionalTransformAction,
    index: &HistoryIndex,
    observed: &BTreeMap<OperationId, Vec<&BrokerObservation>>,
    violations: &mut Vec<Violation>,
) {
    let transforms = index.transactional_transforms.get(&action.transaction_id);
    let Some([transform]) = transforms.map(Vec::as_slice) else {
        violations.push(violation(
            "TXN-007",
            format!(
                "transactional transform {} expected one public completion, observed {}",
                action.transaction_id,
                transforms.map_or(0, Vec::len)
            ),
            Some(action.transaction_id.clone()),
            transform_references(transforms),
        ));
        return;
    };
    verify_public_input(scenario, action, transform, observed, violations);
    match action.disposition {
        TransactionDisposition::Commit => verify_committed(scenario, transform, index, violations),
        TransactionDisposition::Abort => {
            verify_aborted(scenario, action, transform, index, violations);
        }
    }
}

fn verify_public_input(
    scenario: &Scenario,
    action: &TransactionalTransformAction,
    transform: &IndexedTransactionalTransform,
    observed: &BTreeMap<OperationId, Vec<&BrokerObservation>>,
    violations: &mut Vec<Violation>,
) {
    let completion = &transform.completion;
    let expected = sent_record(scenario, &action.expected_input_operation_id);
    let observation = observed
        .get(&action.expected_input_operation_id)
        .and_then(|values| <&[_; 1]>::try_from(values.as_slice()).ok())
        .map(|values| values[0]);
    let group = group_definition(scenario, &action.consumer_id);
    let exact = matches!((expected, observation, completion.records.as_slice(), group),
        (Some(expected), Some(observation), [record], Some((group_id, topic, protocol)))
            if completion.disposition == action.disposition
                && completion.consumer_id == action.consumer_id
                && completion.group_id == group_id
                && completion.topic == topic
                && completion.topic == record.topic
                && completion.partition == record.partition
                && completion.next_offset == record.offset + 1
                && completion.group_epoch.protocol() == protocol
                && completion.group_epoch.is_positive()
                && exact_record(record, expected)
                && record.offset == observation.offset
                && exact_record(record, &observation.record));
    if exact {
        return;
    }
    let mut evidence = vec![format!("history:{}", transform.history_sequence)];
    evidence.extend(
        observed
            .get(&action.expected_input_operation_id)
            .into_iter()
            .flatten()
            .map(|value| format!("broker-observation:{}", value.observation)),
    );
    violations.push(violation(
        "TXN-007",
        format!(
            "transactional transform {} did not retain its exact input record, group fence, and next-offset checkpoint",
            action.transaction_id
        ),
        Some(action.expected_input_operation_id.clone()),
        evidence,
    ));
}

fn verify_committed(
    scenario: &Scenario,
    transform: &IndexedTransactionalTransform,
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) {
    let completion = &transform.completion;
    let probe = scenario.steps.iter().find_map(|step| match &step.action {
        ScenarioAction::ListConsumerGroupOffsets(action)
            if action.group_id == completion.group_id
                && action.topic == completion.topic
                && action.partition == completion.partition
                && action.expected_offset == completion.next_offset
                && index.action_issued(&step.action) =>
        {
            Some(action)
        }
        _ => None,
    });
    let exact = probe.is_some_and(|probe| {
        let public = index
            .consumer_group_offsets_listed
            .get(&probe.operation_id)
            .map(Vec::as_slice);
        let independent = index
            .consumer_group_offsets_observed
            .get(&probe.operation_id)
            .map(Vec::as_slice);
        matches!((public, independent), (Some([public]), Some([independent]))
            if public.history_sequence > transform.history_sequence
                && independent.history_sequence > public.history_sequence
                && public.group_id == completion.group_id
                && public.topic == completion.topic
                && public.partition == completion.partition
                && public.offset == Some(completion.next_offset)
                && independent.group_id == completion.group_id
                && independent.topic == completion.topic
                && independent.partition == completion.partition
                && independent.offset == Some(completion.next_offset))
    });
    if !exact {
        checkpoint_failure(
            transform,
            "committed checkpoint was not independently observed",
            violations,
        );
    }
}

fn verify_aborted(
    scenario: &Scenario,
    action: &TransactionalTransformAction,
    transform: &IndexedTransactionalTransform,
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) {
    let expected = sent_record(scenario, &action.expected_input_operation_id);
    let exact = scenario.steps.iter().any(|step| match &step.action {
        ScenarioAction::GroupReceive {
            consumer_id,
            receive_id,
            expected_operation_id,
            expected_error_code: None,
            ..
        } if expected_operation_id == &action.expected_input_operation_id
            && group_definition(scenario, consumer_id).is_some_and(|(group_id, topic, _)| {
                group_id == transform.completion.group_id && topic == transform.completion.topic
            })
            && index.action_issued(&step.action) =>
        {
            index.receives.get(receive_id).is_some_and(|values| {
                matches!(values.as_slice(), [receive]
                    if receive.history_sequence > transform.history_sequence
                        && receive.committed == Some(true)
                        && matches!((expected, receive.records.as_slice()),
                            (Some(expected), [record]) if exact_record(record, expected)))
            })
        }
        _ => false,
    });
    if !exact {
        checkpoint_failure(
            transform,
            "aborted checkpoint did not redeliver after restart",
            violations,
        );
    }
}

fn group_definition<'a>(
    scenario: &'a Scenario,
    consumer_id: &testlab_schema::ConsumerId,
) -> Option<(&'a str, &'a str, GroupProtocol)> {
    scenario.steps.iter().find_map(|step| match &step.action {
        ScenarioAction::CreateGroupConsumer {
            consumer_id: created,
            group_id,
            topic,
            protocol,
            ..
        } if created == consumer_id => Some((group_id.as_str(), topic.as_str(), *protocol)),
        _ => None,
    })
}

fn checkpoint_failure(
    transform: &IndexedTransactionalTransform,
    message: &str,
    violations: &mut Vec<Violation>,
) {
    violations.push(violation(
        "TXN-008",
        format!(
            "transactional transform {} {message}",
            transform.completion.transaction_id
        ),
        Some(transform.completion.transaction_id.clone()),
        vec![format!("history:{}", transform.history_sequence)],
    ));
}

fn transform_references(values: Option<&Vec<IndexedTransactionalTransform>>) -> Vec<String> {
    values
        .into_iter()
        .flatten()
        .map(|value| format!("history:{}", value.history_sequence))
        .collect()
}
