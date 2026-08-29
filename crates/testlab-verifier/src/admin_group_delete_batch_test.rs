//! Plural group-offset deletion requires ordered results, absence, and prior baselines.

use testlab_schema::{
    AdapterCommand, AdapterEvent, AdminConsumerGroupOffsetMutationOutcome,
    AdminConsumerGroupOffsetsMutation, BrokerConsumerGroupOffset, BrokerStateObservation, ClientId,
    ConsumerGroupOffsetSelection, DeleteConsumerGroupOffsetsAction,
    DeleteConsumerGroupOffsetsCommand, HistoryEntry, HistoryPayload, OperationId, ScenarioAction,
};

use crate::admin_group_multi_test::{
    assert_contract, list_action, list_history, offset_observation, violations,
};
use crate::verify_fixture::{command, event};

#[test]
fn plural_deletion_with_present_correlated_baselines_passes() {
    let baseline = list_action();
    let deletion = delete_action();
    assert!(
        violations(
            vec![baseline.clone(), deletion.clone()],
            &lifecycle_history(&baseline, &deletion),
        )
        .is_empty()
    );
}

#[test]
fn plural_deletion_rejects_missing_baseline() {
    let deletion = delete_action();
    assert_contract(
        &violations(vec![deletion.clone()], &delete_history(&deletion, 0)),
        "ADMIN-026",
    );
}

#[test]
fn plural_deletion_rejects_remaining_offset_or_public_error() {
    let baseline = list_action();
    let deletion = delete_action();
    let mut remaining = lifecycle_history(&baseline, &deletion);
    offset_observation(&mut remaining[8]).offset = Some(9);
    assert_contract(
        &violations(vec![baseline.clone(), deletion.clone()], &remaining),
        "ADMIN-026",
    );

    let mut public_error = lifecycle_history(&baseline, &deletion);
    deleted(&mut public_error).outcomes[0].error_code =
        Some("group_authorization_failed".to_owned());
    assert_contract(
        &violations(vec![baseline, deletion], &public_error),
        "ADMIN-026",
    );
}

fn delete_action() -> ScenarioAction {
    ScenarioAction::DeleteConsumerGroupOffsets(DeleteConsumerGroupOffsetsAction {
        client_id: client(),
        operation_id: operation("delete-offsets"),
        group_id: "group-a".to_owned(),
        partitions: vec![selection("topic-a", 0), selection("topic-c", 2)],
        timeout_ms: 2_000,
    })
}

fn lifecycle_history(baseline: &ScenarioAction, deletion: &ScenarioAction) -> Vec<HistoryEntry> {
    let mut history = list_history(baseline);
    history.extend(delete_history(deletion, 5));
    history
}

fn delete_history(action: &ScenarioAction, start: u64) -> Vec<HistoryEntry> {
    let ScenarioAction::DeleteConsumerGroupOffsets(value) = action else {
        panic!("deletion action fixture");
    };
    vec![
        command(
            start,
            AdapterCommand::DeleteConsumerGroupOffsets(DeleteConsumerGroupOffsetsCommand {
                client_id: value.client_id.clone(),
                operation_id: value.operation_id.clone(),
                group_id: value.group_id.clone(),
                partitions: value.partitions.clone(),
                timeout_ms: value.timeout_ms,
            }),
        ),
        event(
            start + 1,
            AdapterEvent::ConsumerGroupOffsetsDeleted(AdminConsumerGroupOffsetsMutation {
                operation_id: value.operation_id.clone(),
                group_id: value.group_id.clone(),
                outcomes: value
                    .partitions
                    .iter()
                    .map(|value| AdminConsumerGroupOffsetMutationOutcome {
                        topic: value.topic.clone(),
                        partition: value.partition,
                        error_code: None,
                    })
                    .collect(),
            }),
        ),
        offset_state(start + 2, &value.operation_id, "topic-a", 0),
        offset_state(start + 3, &value.operation_id, "topic-c", 2),
    ]
}

fn offset_state(
    sequence: u64,
    operation_id: &OperationId,
    topic: &str,
    partition: i32,
) -> HistoryEntry {
    HistoryEntry {
        sequence,
        observed_unix_ms: sequence,
        payload: HistoryPayload::BrokerStateObservation {
            observation: BrokerStateObservation::ConsumerGroupOffset(BrokerConsumerGroupOffset {
                observation: sequence,
                operation_id: operation_id.clone(),
                group_id: "group-a".to_owned(),
                topic: topic.to_owned(),
                partition,
                offset: None,
            }),
        },
    }
}

fn deleted(history: &mut [HistoryEntry]) -> &mut AdminConsumerGroupOffsetsMutation {
    let HistoryPayload::AdapterEvent { event } = &mut history[6].payload else {
        panic!("deletion event fixture");
    };
    let AdapterEvent::ConsumerGroupOffsetsDeleted(value) = &mut event.event else {
        panic!("deletion outcome fixture");
    };
    value
}

fn selection(topic: &str, partition: i32) -> ConsumerGroupOffsetSelection {
    ConsumerGroupOffsetSelection {
        topic: topic.to_owned(),
        partition,
    }
}

fn client() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client id: {error}"))
}

fn operation(value: &str) -> OperationId {
    OperationId::new(value).unwrap_or_else(|error| panic!("operation id: {error}"))
}
