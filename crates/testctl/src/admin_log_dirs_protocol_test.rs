//! Log-directory protocol tests pin expectation-free exact target identity.

use testlab_schema::{
    AdapterCommand, AdapterEvent, AdminLogDirsDescription, ClientId, DescribeLogDirsAction,
    OperationId, ScenarioAction,
};

use crate::runner_protocol::{EventDisposition, ExpectedEvent};

#[test]
fn translation_keeps_expected_replica_count_off_wire() {
    let action = ScenarioAction::DescribeLogDirs(action());
    let Some((AdapterCommand::DescribeLogDirs(command), expected)) =
        crate::session_command_admin::translate(&action)
    else {
        panic!("log-directory translation");
    };
    assert_eq!(command.client_id, client());
    assert_eq!(command.operation_id, operation());
    assert_eq!(command.topic, "orders");
    assert_eq!(command.partition, 2);
    assert_eq!(command.timeout_ms, 1_000);
    let encoded = serde_json::to_string(&command)
        .unwrap_or_else(|error| panic!("encode log-directory command: {error}"));
    assert!(!encoded.contains("expected_replica_count"));
    assert!(matches!(expected, ExpectedEvent::LogDirsDescribed { .. }));
}

#[test]
fn completion_requires_exact_operation_topic_and_partition() {
    let expected = ExpectedEvent::LogDirsDescribed {
        operation_id: operation(),
        topic: "orders".to_owned(),
        partition: 2,
    };
    let mut event = AdapterEvent::LogDirsDescribed(description());
    assert_eq!(
        expected
            .classify(&event)
            .unwrap_or_else(|error| panic!("classify log-directory event: {error}")),
        EventDisposition::Complete
    );
    let AdapterEvent::LogDirsDescribed(actual) = &mut event else {
        panic!("log-directory event");
    };
    actual.partition = 3;
    let error = expected
        .classify(&event)
        .expect_err("foreign partition must not complete");
    assert_eq!(error.harness_error().code, "event_identity_mismatch");
}

fn action() -> DescribeLogDirsAction {
    DescribeLogDirsAction {
        client_id: client(),
        operation_id: operation(),
        topic: "orders".to_owned(),
        partition: 2,
        expected_replica_count: 1,
        timeout_ms: 1_000,
    }
}

fn description() -> AdminLogDirsDescription {
    AdminLogDirsDescription {
        operation_id: operation(),
        topic: "orders".to_owned(),
        partition: 2,
        throttle_time_ms: 0,
        brokers: Vec::new(),
    }
}

fn client() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client: {error}"))
}

fn operation() -> OperationId {
    OperationId::new("describe-log-dirs").unwrap_or_else(|error| panic!("operation: {error}"))
}
