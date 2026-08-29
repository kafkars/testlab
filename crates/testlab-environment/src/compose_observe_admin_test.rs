//! Compose admin observation tests retain correlation failures without broker access.

use std::time::Duration;

use testlab_schema::{
    AdapterCommand, ClientId, DeleteTopicAction, DeleteTopicCommand, EnvironmentOperationStatus,
    OperationId, ScenarioAction,
};

use crate::compose_test_fixture::Fixture;
use crate::observer_admin_target::AdminTarget;

#[test]
fn unsupported_action_produces_no_environment_operation() {
    let fixture = Fixture::new(false);
    let mut environment = fixture.environment();
    let action = ScenarioAction::CreateClient {
        client_id: client("client-1"),
    };
    let command = AdapterCommand::CreateClient {
        client_id: client("client-1"),
    };

    let observed = environment.observe_admin(&action, &command, Duration::from_millis(1));

    assert!(observed.phase.succeeded());
    assert!(observed.phase.operations.is_empty());
    assert!(observed.state_observations.is_empty());
}

#[test]
fn mismatched_command_is_an_explicit_failed_observation() {
    let fixture = Fixture::new(false);
    let mut environment = fixture.environment();
    let action = ScenarioAction::DeleteTopic(DeleteTopicAction {
        client_id: client("client-1"),
        operation_id: operation("delete-topic"),
        topic: "orders".to_owned(),
        expected_error_code: None,
        timeout_ms: 500,
    });
    let command = AdapterCommand::DeleteTopic(DeleteTopicCommand {
        client_id: client("client-1"),
        operation_id: operation("delete-topic"),
        topic: "other-topic".to_owned(),
        timeout_ms: 500,
    });

    let observed = environment.observe_admin(&action, &command, Duration::from_millis(1));

    assert_eq!(
        observed
            .phase
            .failure
            .as_ref()
            .map(super::compose_types::ComposeFailure::code),
        Some("environment_observation_failed")
    );
    assert_eq!(observed.phase.operations.len(), 1);
    assert_eq!(
        observed.phase.operations[0].status,
        EnvironmentOperationStatus::Failed
    );
    assert!(observed.state_observations.is_empty());
}

#[test]
fn duplicate_admin_operation_identity_is_rejected() {
    let fixture = Fixture::new(false);
    let mut environment = fixture.environment();
    let action = ScenarioAction::DeleteTopic(DeleteTopicAction {
        client_id: client("client-1"),
        operation_id: operation("delete-topic"),
        topic: "orders".to_owned(),
        expected_error_code: None,
        timeout_ms: 500,
    });
    let command = AdapterCommand::DeleteTopic(DeleteTopicCommand {
        client_id: client("client-1"),
        operation_id: operation("delete-topic"),
        topic: "orders".to_owned(),
        timeout_ms: 500,
    });
    let target = AdminTarget::from_exact(&action, &command)
        .unwrap_or_else(|error| panic!("match target: {error}"))
        .unwrap_or_else(|| panic!("missing target"));

    assert_eq!(environment.begin_admin_observation(&target).ok(), Some(0));
    assert!(environment.begin_admin_observation(&target).is_err());
}

fn client(value: &str) -> ClientId {
    ClientId::new(value).unwrap_or_else(|error| panic!("client ID: {error}"))
}

fn operation(value: &str) -> OperationId {
    OperationId::new(value).unwrap_or_else(|error| panic!("operation ID: {error}"))
}
