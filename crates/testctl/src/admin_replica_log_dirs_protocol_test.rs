//! Replica log-directory protocol tests pin expectation-free exact target identity.

use testlab_schema::{
    AdapterCommand, AdapterEvent, AdminReplicaLogDirsAlteration, AdminReplicaLogDirsDescription,
    AlterReplicaLogDirsAction, ClientId, DescribeReplicaLogDirsAction, OperationId,
    ReplicaLogDirAssignmentSpec, ScenarioAction,
};

use crate::runner_protocol::{EventDisposition, ExpectedEvent};

#[test]
fn translation_keeps_expected_replica_count_off_wire() {
    let action = ScenarioAction::DescribeReplicaLogDirs(action());
    let Some((AdapterCommand::DescribeReplicaLogDirs(command), expected)) =
        crate::session_command_admin::translate(&action)
    else {
        panic!("replica log-directory translation");
    };
    assert_eq!(command.client_id, client());
    assert_eq!(command.operation_id, operation());
    assert_eq!(command.topic, "orders");
    assert_eq!(command.partition, 2);
    assert_eq!(command.timeout_ms, 1_000);
    let encoded = serde_json::to_string(&command)
        .unwrap_or_else(|error| panic!("encode replica log-directory command: {error}"));
    assert!(!encoded.contains("expected_replica_count"));
    assert!(matches!(
        expected,
        ExpectedEvent::ReplicaLogDirsDescribed(..)
    ));
}

#[test]
fn alteration_translation_and_completion_preserve_identity() {
    let action = ScenarioAction::AlterReplicaLogDirs(AlterReplicaLogDirsAction {
        client_id: client(),
        operation_id: operation(),
        assignments: vec![assignment(3), assignment(1)],
        timeout_ms: 2_000,
    });
    let Some((AdapterCommand::AlterReplicaLogDirs(command), expected)) =
        crate::session_command_admin::translate(&action)
    else {
        panic!("alter replica log-directory translation");
    };
    assert_eq!(command.assignments, vec![assignment(3), assignment(1)]);
    assert_eq!(command.timeout_ms, 2_000);
    assert_eq!(
        expected
            .classify(&AdapterEvent::ReplicaLogDirsAltered(
                AdminReplicaLogDirsAlteration {
                    operation_id: operation(),
                    throttle_time_ms: 0,
                    outcomes: Vec::new(),
                },
            ))
            .unwrap_or_else(|error| panic!("classify alteration completion: {error}")),
        EventDisposition::Complete
    );
}

fn assignment(broker_id: i32) -> ReplicaLogDirAssignmentSpec {
    ReplicaLogDirAssignmentSpec {
        topic: "orders".to_owned(),
        partition: 2,
        broker_id,
        target_path: "/data-secondary".to_owned(),
    }
}

#[test]
fn completion_requires_exact_operation_topic_and_partition() {
    let expected = ExpectedEvent::ReplicaLogDirsDescribed(operation(), "orders".to_owned(), 2);
    let mut event = AdapterEvent::ReplicaLogDirsDescribed(description());
    assert_eq!(
        expected
            .classify(&event)
            .unwrap_or_else(|error| panic!("classify replica log-directory event: {error}")),
        EventDisposition::Complete
    );
    let AdapterEvent::ReplicaLogDirsDescribed(actual) = &mut event else {
        panic!("replica log-directory event");
    };
    actual.partition = 3;
    let error = expected
        .classify(&event)
        .expect_err("foreign partition must not complete");
    assert_eq!(error.harness_error().code, "event_identity_mismatch");
}

fn action() -> DescribeReplicaLogDirsAction {
    DescribeReplicaLogDirsAction {
        client_id: client(),
        operation_id: operation(),
        topic: "orders".to_owned(),
        partition: 2,
        expected_replica_count: 1,
        timeout_ms: 1_000,
    }
}

fn description() -> AdminReplicaLogDirsDescription {
    AdminReplicaLogDirsDescription {
        operation_id: operation(),
        topic: "orders".to_owned(),
        partition: 2,
        throttle_time_ms: 0,
        replicas: Vec::new(),
    }
}

fn client() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client: {error}"))
}

fn operation() -> OperationId {
    OperationId::new("describe-replica-log-dirs")
        .unwrap_or_else(|error| panic!("operation: {error}"))
}
