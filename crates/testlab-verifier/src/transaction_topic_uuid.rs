//! UUID-bound transaction verification joins public admission to independent topic identity.

use std::collections::BTreeSet;

use testlab_schema::{
    AdapterCommand, BatchRecord, DescribeTopicsAction, OperationId, Scenario, ScenarioAction,
    TopicSelection, TransactionDisposition, Violation,
};

use crate::index::{HistoryIndex, IndexedAdminTopicsDescription, IndexedTopicIdentityObservation};
use crate::support::violation;

pub(crate) fn verify(scenario: &Scenario, index: &HistoryIndex, violations: &mut Vec<Violation>) {
    for step in &scenario.steps {
        let ScenarioAction::ExecuteTransaction {
            producer_id,
            transaction_id,
            operations,
            method,
            disposition,
            topic_identity_operation_id: Some(identity_operation_id),
            timeout_ms,
        } = &step.action
        else {
            continue;
        };
        if !index.action_issued(&step.action) {
            continue;
        }
        let description = scenario.steps.iter().find_map(|step| match &step.action {
            ScenarioAction::DescribeTopics(action)
                if action.operation_id == *identity_operation_id =>
            {
                Some(action)
            }
            _ => None,
        });
        let expected_command = AdapterCommand::ExecuteTransaction {
            producer_id: producer_id.clone(),
            transaction_id: transaction_id.clone(),
            operations: operations.clone(),
            method: *method,
            disposition: *disposition,
            validate_topic_uuids: true,
            timeout_ms: *timeout_ms,
        };
        verify_one(
            transaction_id,
            operations,
            identity_operation_id,
            description,
            &expected_command,
            index,
            violations,
        );
    }
}

fn verify_one(
    transaction_id: &OperationId,
    operations: &[BatchRecord],
    identity_operation_id: &OperationId,
    description: Option<&DescribeTopicsAction>,
    expected_command: &AdapterCommand,
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) {
    let commands = index
        .commands
        .iter()
        .filter(|(_, _, command)| {
            matches!(command, AdapterCommand::ExecuteTransaction {
                transaction_id: actual, ..
            } if actual == transaction_id)
        })
        .collect::<Vec<_>>();
    let public = one(index.topics_batch_described.get(identity_operation_id));
    let independent = index.topic_identities_observed.get(identity_operation_id);
    let completion = one(index.transactions_completed.get(transaction_id));
    let topics = ordered_topics(operations);
    let public_ids = public.and_then(|value| public_ids(value, &topics));
    let independent_ids = independent.and_then(|values| independent_ids(values, &topics));
    let command_sequence =
        matches!(commands.as_slice(), [(_, _, actual)] if actual == expected_command)
            .then(|| commands[0].0);
    let identities_precede_command = command_sequence.is_some_and(|command| {
        public.is_some_and(|value| value.history_sequence < command)
            && independent.is_some_and(|values| {
                !values.is_empty()
                    && public.is_some_and(|public| {
                        values.iter().all(|value| {
                            public.history_sequence < value.history_sequence
                                && value.history_sequence < command
                        })
                    })
            })
    });
    let exact = description.is_some_and(|action| {
        action.selection == TopicSelection::TopicId
            && action.operation_id == *identity_operation_id
            && action.topics.len() == topics.len()
            && action
                .topics
                .iter()
                .zip(&topics)
                .all(|(expected, actual)| expected.topic == *actual)
    }) && command_sequence.is_some_and(|command| {
        completion.is_some_and(|value| {
            value.history_sequence > command
                && value.disposition == TransactionDisposition::Commit
                && public_ids.as_ref() == Some(&value.validated_topic_ids)
        })
    }) && identities_precede_command
        && public_ids.is_some()
        && public_ids == independent_ids
        && public_ids
            .as_ref()
            .is_some_and(|ids| ids.len() == ids.iter().copied().collect::<BTreeSet<_>>().len());
    if exact {
        return;
    }
    violations.push(violation(
        "TXN-010",
        format!(
            "transaction {transaction_id} expected one exact UUID-validating commit after ordered public and independent topic-ID evidence"
        ),
        Some(transaction_id.clone()),
        commands
            .iter()
            .map(|(sequence, ..)| format!("history:{sequence}"))
            .chain(
                public
                    .map(|value| format!("history:{}", value.history_sequence)),
            )
            .chain(independent.into_iter().flatten().map(|value| {
                format!("broker-state-observation:{}", value.observation)
            }))
            .chain(
                completion
                    .map(|value| format!("history:{}", value.history_sequence)),
            )
            .collect(),
    ));
}

fn ordered_topics(operations: &[BatchRecord]) -> Vec<&str> {
    let mut seen = BTreeSet::new();
    operations
        .iter()
        .filter_map(|operation| {
            let topic = operation.record.topic.as_str();
            seen.insert(topic).then_some(topic)
        })
        .collect()
}

fn public_ids(value: &IndexedAdminTopicsDescription, topics: &[&str]) -> Option<Vec<[u8; 16]>> {
    if value.value.outcomes.len() != topics.len() {
        return None;
    }
    value
        .value
        .outcomes
        .iter()
        .zip(topics)
        .map(|(outcome, topic)| {
            let id = outcome.topic_id?;
            (outcome.topic == *topic
                && id != [0; 16]
                && outcome.error_code.is_none()
                && outcome
                    .description
                    .as_ref()
                    .is_some_and(|description| description.topic_id == Some(id)))
            .then_some(id)
        })
        .collect()
}

fn independent_ids(
    values: &[IndexedTopicIdentityObservation],
    topics: &[&str],
) -> Option<Vec<[u8; 16]>> {
    if values.len() != topics.len() {
        return None;
    }
    values
        .iter()
        .zip(topics)
        .map(|(value, topic)| {
            (value.topic == *topic && value.topic_id != [0; 16]).then_some(value.topic_id)
        })
        .collect()
}

fn one<T>(values: Option<&Vec<T>>) -> Option<&T> {
    let [value] = values?.as_slice() else {
        return None;
    };
    Some(value)
}
