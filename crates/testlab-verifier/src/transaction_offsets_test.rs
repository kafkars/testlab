//! Transactional offset tests cover public fence and checkpoint evidence.

use testlab_schema::{
    AdapterCommand, AdapterEvent, AdminConsumerGroupOffsetListing, BrokerConsumerGroupOffset,
    BrokerObservation, BrokerStateObservation, ConsumedRecord, GroupMembershipEpoch, HistoryEntry,
    HistoryPayload, ListConsumerGroupOffsetsCommand, Scenario, ScenarioAction,
    TransactionalTransformAction, TransactionalTransformCompletion,
};

use crate::index::HistoryIndex;
use crate::verify_fixture::{command, event};
use crate::verify_index::observations_by_operation;

#[test]
fn committed_transform_requires_independent_checkpoint_evidence() {
    let scenario = scenario();
    let action = transform(&scenario, 6);
    let record = consumed(&scenario, &action.expected_input_operation_id, 0);
    let completion = completion(&scenario, action, &record);
    let ScenarioAction::ListConsumerGroupOffsets(probe) = &scenario.steps[8].action else {
        panic!("committed checkpoint probe missing");
    };
    let mut history = vec![event(
        10,
        AdapterEvent::TransactionalTransformCompleted(completion),
    )];
    history.push(command(
        19,
        AdapterCommand::ListConsumerGroupOffsets(ListConsumerGroupOffsetsCommand {
            client_id: probe.client_id.clone(),
            operation_id: probe.operation_id.clone(),
            group_id: probe.group_id.clone(),
            topic: probe.topic.clone(),
            partition: probe.partition,
            require_stable: probe.require_stable,
            timeout_ms: probe.timeout_ms,
        }),
    ));
    history.push(event(
        20,
        AdapterEvent::ConsumerGroupOffsetListed(AdminConsumerGroupOffsetListing {
            operation_id: probe.operation_id.clone(),
            group_id: probe.group_id.clone(),
            topic: probe.topic.clone(),
            partition: probe.partition,
            offset: Some(probe.expected_offset),
        }),
    ));
    let observed = [observation(
        &scenario,
        &action.expected_input_operation_id,
        0,
    )];

    let violations = verify_action(&scenario, action, &history, &observed);

    assert!(
        violations
            .iter()
            .any(|value| value.contract_id.as_str() == "TXN-008")
    );

    history.push(independent_offset(
        21,
        probe.operation_id.clone(),
        &probe.group_id,
        &probe.topic,
        probe.partition,
        probe.expected_offset,
    ));
    let index = HistoryIndex::build(&history);
    assert_eq!(
        index
            .consumer_group_offsets_listed
            .get(&probe.operation_id)
            .map(Vec::len),
        Some(1)
    );
    assert_eq!(
        index
            .consumer_group_offsets_observed
            .get(&probe.operation_id)
            .map(Vec::len),
        Some(1)
    );
    assert!(index.action_issued(&scenario.steps[8].action));
    let public = &index.consumer_group_offsets_listed[&probe.operation_id][0];
    let independent = &index.consumer_group_offsets_observed[&probe.operation_id][0];
    assert_eq!(public.group_id, probe.group_id);
    assert_eq!(public.topic, probe.topic);
    assert_eq!(public.partition, probe.partition);
    assert_eq!(public.offset, Some(probe.expected_offset));
    assert_eq!(independent.group_id, probe.group_id);
    assert_eq!(independent.topic, probe.topic);
    assert_eq!(independent.partition, probe.partition);
    assert_eq!(independent.offset, Some(probe.expected_offset));
    assert!(public.history_sequence > 10);
    assert!(independent.history_sequence > public.history_sequence);
    let violations = verify_action(&scenario, action, &history, &observed);
    assert!(violations.is_empty(), "{violations:?}");
}

#[test]
fn aborted_transform_requires_exact_post_restart_redelivery() {
    let scenario = scenario();
    let action = transform(&scenario, 11);
    let record = consumed(&scenario, &action.expected_input_operation_id, 1);
    let completion = completion(&scenario, action, &record);
    let receive_id = match &scenario.steps[14].action {
        ScenarioAction::GroupReceive { receive_id, .. } => receive_id.clone(),
        _ => panic!("abort redelivery missing"),
    };
    let mut history = vec![event(
        10,
        AdapterEvent::TransactionalTransformCompleted(completion),
    )];
    let observed = [observation(
        &scenario,
        &action.expected_input_operation_id,
        1,
    )];

    let violations = verify_action(&scenario, action, &history, &observed);
    assert!(
        violations
            .iter()
            .any(|value| value.contract_id.as_str() == "TXN-008")
    );

    history.push(event(
        20,
        AdapterEvent::GroupReceiveCompleted {
            receive_id,
            records: vec![record],
            committed: true,
            group_epoch: Some(GroupMembershipEpoch::Classic { generation_id: 2 }),
        },
    ));
    let violations = verify_action(&scenario, action, &history, &observed);
    assert!(violations.is_empty(), "{violations:?}");
}

fn scenario() -> Scenario {
    toml::from_str(include_str!(
        "../../../scenarios/kafka/transactional-offset-classic.toml"
    ))
    .unwrap_or_else(|error| panic!("parse transactional offset scenario: {error}"))
}

fn transform(scenario: &Scenario, index: usize) -> &TransactionalTransformAction {
    match &scenario.steps[index].action {
        ScenarioAction::ExecuteTransactionalTransform(action) => action,
        _ => panic!("transactional transform missing"),
    }
}

fn completion(
    scenario: &Scenario,
    action: &TransactionalTransformAction,
    record: &ConsumedRecord,
) -> TransactionalTransformCompletion {
    let (group_id, topic, protocol) = scenario
        .steps
        .iter()
        .find_map(|step| match &step.action {
            ScenarioAction::CreateGroupConsumer {
                consumer_id,
                group_id,
                topic,
                protocol,
                ..
            } if consumer_id == &action.consumer_id => Some((group_id, topic, protocol)),
            _ => None,
        })
        .unwrap_or_else(|| panic!("group definition missing"));
    let group_epoch = match protocol {
        testlab_schema::GroupProtocol::Classic => {
            GroupMembershipEpoch::Classic { generation_id: 1 }
        }
        testlab_schema::GroupProtocol::Consumer => {
            GroupMembershipEpoch::Consumer { member_epoch: 1 }
        }
    };
    TransactionalTransformCompletion {
        transaction_id: action.transaction_id.clone(),
        disposition: action.disposition,
        consumer_id: action.consumer_id.clone(),
        records: vec![record.clone()],
        group_id: group_id.clone(),
        topic: topic.clone(),
        partition: record.partition,
        next_offset: record.offset + 1,
        group_epoch,
    }
}

fn consumed(
    scenario: &Scenario,
    operation_id: &testlab_schema::OperationId,
    offset: i64,
) -> ConsumedRecord {
    let record = crate::consumer::sent_record(scenario, operation_id)
        .unwrap_or_else(|| panic!("source record missing"));
    ConsumedRecord {
        topic: record.topic.clone(),
        partition: record.partition,
        offset,
        timestamp_millis: None,
        key: record.key.clone(),
        value: record.value.clone(),
        headers: record.headers.clone(),
    }
}

fn observation(
    scenario: &Scenario,
    operation_id: &testlab_schema::OperationId,
    offset: i64,
) -> BrokerObservation {
    let record = crate::consumer::sent_record(scenario, operation_id)
        .unwrap_or_else(|| panic!("source record missing"))
        .clone();
    let digest = record
        .digest()
        .unwrap_or_else(|error| panic!("source digest: {error}"));
    BrokerObservation {
        observation: 1,
        offset,
        operation_id: operation_id.clone(),
        digest,
        record,
    }
}

fn independent_offset(
    sequence: u64,
    operation_id: testlab_schema::OperationId,
    group_id: &str,
    topic: &str,
    partition: i32,
    offset: i64,
) -> HistoryEntry {
    HistoryEntry {
        sequence,
        observed_unix_ms: sequence,
        payload: HistoryPayload::BrokerStateObservation {
            observation: BrokerStateObservation::ConsumerGroupOffset(BrokerConsumerGroupOffset {
                observation: sequence,
                operation_id,
                group_id: group_id.to_owned(),
                topic: topic.to_owned(),
                partition,
                offset: Some(offset),
            }),
        },
    }
}

fn verify_action(
    scenario: &Scenario,
    action: &TransactionalTransformAction,
    history: &[HistoryEntry],
    observations: &[BrokerObservation],
) -> Vec<testlab_schema::Violation> {
    let index = HistoryIndex::build(history);
    let observed = observations_by_operation(observations);
    let mut violations = Vec::new();
    crate::transaction_offsets::verify(scenario, action, &index, &observed, &mut violations);
    violations
}
