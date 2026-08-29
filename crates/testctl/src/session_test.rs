//! Session tests preserve failed-scenario cleanup as an explicit abort.

use testlab_schema::{
    AdapterCommand, ClientId, CreatePartitionsAction, CreateTopicAction, OperationId,
    ScenarioAction, TOPIC_ALREADY_EXISTS_ERROR_CODE, UNKNOWN_TOPIC_OR_PARTITION_ERROR_CODE,
};

use super::runner_protocol::ExpectedEvent;
use super::session::{expects_admin_failure, scenario_failure_settlement};

#[test]
fn scenario_failure_aborts_instead_of_claiming_clean_finish() {
    let (command, expected) = scenario_failure_settlement();

    assert_eq!(command, AdapterCommand::Abort);
    assert!(matches!(expected, ExpectedEvent::Aborted));
}

#[test]
fn only_declared_admin_errors_observe_after_public_failure() {
    let mut action = CreateTopicAction {
        client_id: ClientId::new("client-1").unwrap_or_else(|error| panic!("client: {error}")),
        operation_id: OperationId::new("duplicate-topic")
            .unwrap_or_else(|error| panic!("operation: {error}")),
        topic: "orders".to_owned(),
        partitions: 1,
        replication_factor: 1,
        validate_only: false,
        expected_error_code: None,
        timeout_ms: 1_000,
    };

    assert!(!expects_admin_failure(&ScenarioAction::CreateTopic(
        action.clone()
    )));
    action.expected_error_code = Some(TOPIC_ALREADY_EXISTS_ERROR_CODE.to_owned());
    assert!(expects_admin_failure(&ScenarioAction::CreateTopic(action)));

    assert!(expects_admin_failure(&ScenarioAction::CreatePartitions(
        CreatePartitionsAction {
            client_id: ClientId::new("client-1").unwrap_or_else(|error| panic!("client: {error}")),
            operation_id: OperationId::new("missing-topic")
                .unwrap_or_else(|error| panic!("operation: {error}")),
            topic: "missing-orders".to_owned(),
            total_count: 2,
            validate_only: false,
            expected_current_count: None,
            expected_error_code: Some(UNKNOWN_TOPIC_OR_PARTITION_ERROR_CODE.to_owned()),
            timeout_ms: 1_000,
        }
    )));
}
