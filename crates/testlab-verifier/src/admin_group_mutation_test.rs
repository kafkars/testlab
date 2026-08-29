//! Group-offset mutation tests require exact completions and independently observed postconditions.

use testlab_schema::{
    AdapterCommand, AdapterEvent, AdminConsumerGroupOffsetCompletion,
    AlterConsumerGroupOffsetAction, AlterConsumerGroupOffsetCommand, BrokerConsumerGroupOffset,
    BrokerStateObservation, DeleteConsumerGroupOffsetAction, DeleteConsumerGroupOffsetCommand,
    HistoryEntry, HistoryPayload, OperationId, ScenarioAction, TerminalStatus,
    VisibilityExpectation,
};

use crate::admin::verify_admin;
use crate::index::HistoryIndex;
use crate::verify_fixture::{command, event, scenario, step};

#[test]
fn altered_group_offset_with_matching_postcondition_passes() {
    assert!(violations(alter_action(), &alter_history(1, Some(9))).is_empty());
}

#[test]
fn offset_alteration_rejects_duplicate_public_results_or_wrong_state() {
    for history in [alter_history(2, Some(9)), alter_history(1, Some(8))] {
        assert_contract(&violations(alter_action(), &history), "ADMIN-011");
    }
}

#[test]
fn deleted_group_offset_with_independent_absence_passes() {
    assert!(violations(delete_action(), &delete_history(None)).is_empty());
}

#[test]
fn offset_deletion_rejects_a_remaining_committed_value() {
    assert_contract(
        &violations(delete_action(), &delete_history(Some(9))),
        "ADMIN-012",
    );
}

fn alter_history(public_count: usize, observed_offset: Option<i64>) -> Vec<HistoryEntry> {
    let operation_id = operation("admin-alter-offset-1");
    let mut history = vec![command(
        0,
        AdapterCommand::AlterConsumerGroupOffset(AlterConsumerGroupOffsetCommand {
            client_id: client(),
            operation_id: operation_id.clone(),
            group_id: "group-a".to_owned(),
            topic: "records".to_owned(),
            partition: 0,
            offset: 9,
            timeout_ms: 1_000,
        }),
    )];
    for sequence in 1..=public_count {
        history.push(event(
            sequence as u64,
            AdapterEvent::ConsumerGroupOffsetAltered(offset_completion(operation_id.clone())),
        ));
    }
    let sequence = public_count as u64 + 1;
    history.push(offset_state(sequence, operation_id, observed_offset));
    history
}

fn delete_history(observed_offset: Option<i64>) -> Vec<HistoryEntry> {
    let operation_id = operation("admin-delete-offset-1");
    vec![
        command(
            0,
            AdapterCommand::DeleteConsumerGroupOffset(DeleteConsumerGroupOffsetCommand {
                client_id: client(),
                operation_id: operation_id.clone(),
                group_id: "group-a".to_owned(),
                topic: "records".to_owned(),
                partition: 0,
                timeout_ms: 1_000,
            }),
        ),
        event(
            1,
            AdapterEvent::ConsumerGroupOffsetDeleted(offset_completion(operation_id.clone())),
        ),
        offset_state(2, operation_id, observed_offset),
    ]
}

fn alter_action() -> ScenarioAction {
    ScenarioAction::AlterConsumerGroupOffset(AlterConsumerGroupOffsetAction {
        client_id: client(),
        operation_id: operation("admin-alter-offset-1"),
        group_id: "group-a".to_owned(),
        topic: "records".to_owned(),
        partition: 0,
        offset: 9,
        timeout_ms: 1_000,
    })
}

fn delete_action() -> ScenarioAction {
    ScenarioAction::DeleteConsumerGroupOffset(DeleteConsumerGroupOffsetAction {
        client_id: client(),
        operation_id: operation("admin-delete-offset-1"),
        group_id: "group-a".to_owned(),
        topic: "records".to_owned(),
        partition: 0,
        timeout_ms: 1_000,
    })
}

fn offset_completion(operation_id: OperationId) -> AdminConsumerGroupOffsetCompletion {
    AdminConsumerGroupOffsetCompletion {
        operation_id,
        group_id: "group-a".to_owned(),
        topic: "records".to_owned(),
        partition: 0,
    }
}

fn offset_state(sequence: u64, operation_id: OperationId, offset: Option<i64>) -> HistoryEntry {
    HistoryEntry {
        sequence,
        observed_unix_ms: sequence,
        payload: HistoryPayload::BrokerStateObservation {
            observation: BrokerStateObservation::ConsumerGroupOffset(BrokerConsumerGroupOffset {
                observation: sequence,
                operation_id,
                group_id: "group-a".to_owned(),
                topic: "records".to_owned(),
                partition: 0,
                offset,
            }),
        },
    }
}

fn violations(action: ScenarioAction, history: &[HistoryEntry]) -> Vec<testlab_schema::Violation> {
    let mut scenario = scenario(
        TerminalStatus::Acknowledged,
        VisibilityExpectation::ExactlyOnce,
    );
    scenario.steps.insert(2, step("admin-operation", action));
    let index = HistoryIndex::build(history);
    let mut violations = Vec::new();
    verify_admin(&scenario, &index, &[], &mut violations);
    violations
}

fn assert_contract(violations: &[testlab_schema::Violation], contract: &str) {
    assert!(
        violations
            .iter()
            .any(|violation| violation.contract_id.as_str() == contract),
        "{violations:?}"
    );
}

fn client() -> testlab_schema::ClientId {
    testlab_schema::ClientId::new("client-1").unwrap_or_else(|error| panic!("client id: {error}"))
}

fn operation(value: &str) -> OperationId {
    OperationId::new(value).unwrap_or_else(|error| panic!("operation id: {error}"))
}
