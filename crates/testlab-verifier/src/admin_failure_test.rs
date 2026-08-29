//! Expected admin failure tests bind exact public errors to independent topic state.

use testlab_schema::{
    AdapterCommand, AdapterEvent, AdminOffsetPosition, BrokerStateObservation, BrokerTopicState,
    ClientId, CreatePartitionsAction, CreatePartitionsCommand, DeleteTopicAction,
    DeleteTopicCommand, DescribeTopicAction, DescribeTopicCommand, HistoryEntry, HistoryPayload,
    ListOffsetsAction, ListOffsetsCommand, OperationId, ScenarioAction, TerminalStatus,
    UNKNOWN_TOPIC_OR_PARTITION_ERROR_CODE, VisibilityExpectation,
};

use crate::admin::verify_admin;
use crate::index::HistoryIndex;
use crate::verify_fixture::{command, event, scenario, step};

#[test]
fn exact_missing_resource_failures_with_unchanged_state_pass() {
    for action in failure_actions() {
        assert!(violations(&action, expected_failure(), state(&action)).is_empty());
    }
}

#[test]
fn wrong_code_success_or_changed_state_fails() {
    let action = failure_actions().remove(0);
    let wrong_code = AdapterEvent::CommandFailed {
        code: "broker:broker_36".to_owned(),
        diagnostic: "wrong resource error".to_owned(),
    };
    assert_contract(&violations(&action, wrong_code, state(&action)));

    let success = AdapterEvent::TopicPartitionsCreated(testlab_schema::AdminTopicCompletion {
        operation_id: operation("missing-partitions"),
        topic: "missing-partitions".to_owned(),
    });
    assert_contract(&violations(&action, success, state(&action)));

    let changed = topic_state(2, &action, true, vec![0]);
    assert_contract(&violations(&action, expected_failure(), changed));
}

fn failure_actions() -> Vec<ScenarioAction> {
    let code = || Some(UNKNOWN_TOPIC_OR_PARTITION_ERROR_CODE.to_owned());
    vec![
        ScenarioAction::CreatePartitions(CreatePartitionsAction {
            client_id: client(),
            operation_id: operation("missing-partitions"),
            topic: "missing-partitions".to_owned(),
            total_count: 2,
            validate_only: false,
            expected_current_count: None,
            expected_error_code: code(),
            timeout_ms: 1_000,
        }),
        ScenarioAction::DeleteTopic(DeleteTopicAction {
            client_id: client(),
            operation_id: operation("missing-delete"),
            topic: "missing-delete".to_owned(),
            expected_error_code: code(),
            timeout_ms: 1_000,
        }),
        ScenarioAction::DescribeTopic(DescribeTopicAction {
            client_id: client(),
            operation_id: operation("missing-describe"),
            topic: "missing-describe".to_owned(),
            expected_partitions: None,
            expected_error_code: code(),
            timeout_ms: 1_000,
        }),
        ScenarioAction::ListOffsets(ListOffsetsAction {
            client_id: client(),
            operation_id: operation("missing-partition"),
            topic: "missing-partition".to_owned(),
            partition: 1,
            position: AdminOffsetPosition::Latest,
            expected_offset: None,
            expected_error_code: code(),
            timeout_ms: 1_000,
        }),
    ]
}

fn violations(
    action: &ScenarioAction,
    result: AdapterEvent,
    state: HistoryEntry,
) -> Vec<testlab_schema::Violation> {
    let mut scenario = scenario(
        TerminalStatus::Acknowledged,
        VisibilityExpectation::ExactlyOnce,
    );
    scenario
        .steps
        .insert(2, step("admin-failure", action.clone()));
    let entries = [command(0, wire(action)), event(1, result), state];
    let index = HistoryIndex::build(&entries);
    let mut violations = Vec::new();
    verify_admin(&scenario, &index, &[], &mut violations);
    violations
}

fn wire(action: &ScenarioAction) -> AdapterCommand {
    match action {
        ScenarioAction::CreatePartitions(action) => {
            AdapterCommand::CreatePartitions(CreatePartitionsCommand {
                client_id: action.client_id.clone(),
                operation_id: action.operation_id.clone(),
                topic: action.topic.clone(),
                total_count: action.total_count,
                validate_only: action.validate_only,
                timeout_ms: action.timeout_ms,
            })
        }
        ScenarioAction::DeleteTopic(action) => AdapterCommand::DeleteTopic(DeleteTopicCommand {
            client_id: action.client_id.clone(),
            operation_id: action.operation_id.clone(),
            topic: action.topic.clone(),
            timeout_ms: action.timeout_ms,
        }),
        ScenarioAction::DescribeTopic(action) => {
            AdapterCommand::DescribeTopic(DescribeTopicCommand {
                client_id: action.client_id.clone(),
                operation_id: action.operation_id.clone(),
                topic: action.topic.clone(),
                timeout_ms: action.timeout_ms,
            })
        }
        ScenarioAction::ListOffsets(action) => AdapterCommand::ListOffsets(ListOffsetsCommand {
            client_id: action.client_id.clone(),
            operation_id: action.operation_id.clone(),
            topic: action.topic.clone(),
            partition: action.partition,
            position: action.position,
            timeout_ms: action.timeout_ms,
        }),
        _ => panic!("unexpected failure action"),
    }
}

fn state(action: &ScenarioAction) -> HistoryEntry {
    if matches!(action, ScenarioAction::ListOffsets(_)) {
        topic_state(2, action, true, vec![0])
    } else {
        topic_state(2, action, false, Vec::new())
    }
}

fn topic_state(
    sequence: u64,
    action: &ScenarioAction,
    exists: bool,
    partitions: Vec<i32>,
) -> HistoryEntry {
    let (operation_id, topic) = action_identity(action);
    HistoryEntry {
        sequence,
        observed_unix_ms: sequence,
        payload: HistoryPayload::BrokerStateObservation {
            observation: BrokerStateObservation::Topic(BrokerTopicState {
                observation: sequence,
                operation_id: operation_id.clone(),
                topic: topic.to_owned(),
                exists,
                partitions,
            }),
        },
    }
}

fn action_identity(action: &ScenarioAction) -> (&OperationId, &str) {
    match action {
        ScenarioAction::CreatePartitions(action) => (&action.operation_id, &action.topic),
        ScenarioAction::DeleteTopic(action) => (&action.operation_id, &action.topic),
        ScenarioAction::DescribeTopic(action) => (&action.operation_id, &action.topic),
        ScenarioAction::ListOffsets(action) => (&action.operation_id, &action.topic),
        _ => panic!("unexpected failure action"),
    }
}

fn expected_failure() -> AdapterEvent {
    AdapterEvent::CommandFailed {
        code: UNKNOWN_TOPIC_OR_PARTITION_ERROR_CODE.to_owned(),
        diagnostic: "unknown topic or partition".to_owned(),
    }
}

fn assert_contract(violations: &[testlab_schema::Violation]) {
    assert!(
        violations
            .iter()
            .any(|value| value.contract_id.as_str() == "ADMIN-019"),
        "{violations:?}"
    );
}

fn client() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client: {error}"))
}

fn operation(value: &str) -> OperationId {
    OperationId::new(value).unwrap_or_else(|error| panic!("operation: {error}"))
}
