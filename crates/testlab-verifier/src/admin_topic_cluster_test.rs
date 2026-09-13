//! Topic-deletion and cluster-description tests join public results to metadata snapshots.

use testlab_schema::{
    AdapterCommand, AdapterEvent, AdminTopicCompletion, BrokerStateObservation, BrokerTopicState,
    DeleteTopicAction, DeleteTopicCommand, HistoryEntry, HistoryPayload, OperationId,
    ScenarioAction, TerminalStatus, VisibilityExpectation,
};

use crate::admin::verify_admin;
use crate::index::HistoryIndex;
use crate::verify_fixture::{command, event, scenario, step};

const TOPIC_OPERATION: &str = "admin-delete-topic-1";

#[path = "admin_cluster_description_test.rs"]
mod cluster_tests;

#[test]
fn deleted_topic_with_independent_absence_passes() {
    let history = topic_history(1, false, Vec::new());
    assert!(violations(delete_topic_action(), &history).is_empty());
}

#[test]
fn topic_deletion_rejects_duplicate_public_results_or_present_state() {
    for history in [
        topic_history(2, false, Vec::new()),
        topic_history(1, true, vec![0]),
    ] {
        assert_contract(&violations(delete_topic_action(), &history), "ADMIN-007");
    }
}

#[test]
fn immediate_state_must_precede_the_next_harness_command() {
    let mut history = topic_history(1, false, Vec::new());
    history.insert(2, command(2, AdapterCommand::Finish));
    let Some(state) = history.last_mut() else {
        panic!("topic history omitted its state observation");
    };
    state.sequence = 3;
    state.observed_unix_ms = 3;
    assert_contract(&violations(delete_topic_action(), &history), "ADMIN-007");
}

fn topic_history(public_count: usize, exists: bool, partitions: Vec<i32>) -> Vec<HistoryEntry> {
    let operation_id = operation(TOPIC_OPERATION);
    let mut history = vec![command(
        0,
        AdapterCommand::DeleteTopic(DeleteTopicCommand {
            client_id: client(),
            operation_id: operation_id.clone(),
            topic: "records".to_owned(),
            timeout_ms: 1_000,
        }),
    )];
    for sequence in 1..=public_count {
        history.push(event(
            sequence as u64,
            AdapterEvent::TopicDeleted(AdminTopicCompletion {
                operation_id: operation_id.clone(),
                topic: "records".to_owned(),
            }),
        ));
    }
    let sequence = public_count as u64 + 1;
    history.push(state(
        sequence,
        BrokerStateObservation::Topic(BrokerTopicState {
            observation: sequence,
            operation_id,
            topic: "records".to_owned(),
            exists,
            partitions,
        }),
    ));
    history
}

fn delete_topic_action() -> ScenarioAction {
    ScenarioAction::DeleteTopic(DeleteTopicAction {
        client_id: client(),
        operation_id: operation(TOPIC_OPERATION),
        topic: "records".to_owned(),
        expected_error_code: None,
        timeout_ms: 1_000,
    })
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

fn state(sequence: u64, observation: BrokerStateObservation) -> HistoryEntry {
    HistoryEntry {
        sequence,
        observed_unix_ms: sequence,
        payload: HistoryPayload::BrokerStateObservation { observation },
    }
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
