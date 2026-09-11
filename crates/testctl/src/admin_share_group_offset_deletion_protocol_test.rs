//! Share-group offset deletion protocol tests pin topic-wide public identity.

use testlab_schema::{
    AdapterCommand, AdapterEvent, AdminShareGroupOffsetDeletion, ClientId,
    DeleteShareGroupOffsetsAction, OperationId, ScenarioAction,
};

use crate::runner_protocol::{EventDisposition, ExpectedEvent};

#[test]
fn translation_keeps_scenario_partition_off_the_wire() {
    let scenario = ScenarioAction::DeleteShareGroupOffsets(action());
    let Some((AdapterCommand::DeleteShareGroupOffsets(command), expected)) =
        crate::session_command_admin_share_group::translate(&scenario)
    else {
        panic!("Share-group offset deletion translation");
    };
    assert_eq!(command.client_id, client());
    assert_eq!(command.operation_id, operation());
    assert_eq!(command.group_id, "share-group-1");
    assert_eq!(command.topic, "share-topic");
    assert_eq!(command.timeout_ms, 1_000);
    assert!(matches!(
        expected,
        ExpectedEvent::ShareGroupOffsetsDeleted { .. }
    ));
}

#[test]
fn completion_requires_exact_topic_identity() {
    let expected = ExpectedEvent::ShareGroupOffsetsDeleted {
        operation_id: operation(),
        group_id: "share-group-1".to_owned(),
        topic: "share-topic".to_owned(),
    };
    let mut event = AdapterEvent::ShareGroupOffsetsDeleted(deletion());
    assert_eq!(
        expected
            .classify(&event)
            .unwrap_or_else(|error| panic!("classify deletion: {error}")),
        EventDisposition::Complete
    );
    let AdapterEvent::ShareGroupOffsetsDeleted(actual) = &mut event else {
        panic!("Share-group offset deletion event");
    };
    actual.topic = "foreign-topic".to_owned();
    let error = expected
        .classify(&event)
        .expect_err("foreign topic must not complete");
    assert_eq!(error.harness_error().code, "event_identity_mismatch");
}

fn action() -> DeleteShareGroupOffsetsAction {
    DeleteShareGroupOffsetsAction {
        client_id: client(),
        operation_id: operation(),
        group_id: "share-group-1".to_owned(),
        topic: "share-topic".to_owned(),
        partition: 0,
        timeout_ms: 1_000,
    }
}

fn deletion() -> AdminShareGroupOffsetDeletion {
    AdminShareGroupOffsetDeletion {
        operation_id: operation(),
        group_id: "share-group-1".to_owned(),
        topic: "share-topic".to_owned(),
        topic_id: [1; 16],
        error_code: None,
    }
}

fn client() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client: {error}"))
}

fn operation() -> OperationId {
    OperationId::new("delete-share-group-offsets")
        .unwrap_or_else(|error| panic!("operation: {error}"))
}
