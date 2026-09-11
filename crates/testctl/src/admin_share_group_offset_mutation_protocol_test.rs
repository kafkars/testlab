//! Share-group offset mutation protocol tests pin intent and result identities.

use testlab_schema::{
    AdapterCommand, AdapterEvent, AdminShareGroupOffsetAlteration, AlterShareGroupOffsetsAction,
    ClientId, OperationId, ScenarioAction,
};

use crate::runner_protocol::{EventDisposition, ExpectedEvent};

#[test]
fn translation_keeps_expected_lag_off_the_wire() {
    let scenario = ScenarioAction::AlterShareGroupOffsets(action());
    let Some((AdapterCommand::AlterShareGroupOffsets(command), expected)) =
        crate::session_command_admin_share_group::translate(&scenario)
    else {
        panic!("Share-group offset alteration translation");
    };
    assert_eq!(command.client_id, client());
    assert_eq!(command.operation_id, operation());
    assert_eq!(command.group_id, "share-group-1");
    assert_eq!(command.topic, "share-topic");
    assert_eq!(command.partition, 0);
    assert_eq!(command.start_offset, 2);
    assert_eq!(command.timeout_ms, 1_000);
    assert!(matches!(
        expected,
        ExpectedEvent::ShareGroupOffsetsAltered { .. }
    ));
}

#[test]
fn completion_requires_exact_partition_identity() {
    let expected = ExpectedEvent::ShareGroupOffsetsAltered {
        operation_id: operation(),
        group_id: "share-group-1".to_owned(),
        topic: "share-topic".to_owned(),
        partition: 0,
    };
    let mut event = AdapterEvent::ShareGroupOffsetsAltered(alteration());
    assert_eq!(
        expected
            .classify(&event)
            .unwrap_or_else(|error| panic!("classify alteration: {error}")),
        EventDisposition::Complete
    );
    let AdapterEvent::ShareGroupOffsetsAltered(actual) = &mut event else {
        panic!("Share-group offset alteration event");
    };
    actual.partition = 1;
    let error = expected
        .classify(&event)
        .expect_err("foreign partition must not complete");
    assert_eq!(error.harness_error().code, "event_identity_mismatch");
}

fn action() -> AlterShareGroupOffsetsAction {
    AlterShareGroupOffsetsAction {
        client_id: client(),
        operation_id: operation(),
        group_id: "share-group-1".to_owned(),
        topic: "share-topic".to_owned(),
        partition: 0,
        start_offset: 2,
        expected_lag: 0,
        timeout_ms: 1_000,
    }
}

fn alteration() -> AdminShareGroupOffsetAlteration {
    AdminShareGroupOffsetAlteration {
        operation_id: operation(),
        group_id: "share-group-1".to_owned(),
        topic: "share-topic".to_owned(),
        partition: 0,
        topic_id: [1; 16],
        error_code: None,
    }
}

fn client() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client: {error}"))
}

fn operation() -> OperationId {
    OperationId::new("alter-share-group-offsets")
        .unwrap_or_else(|error| panic!("operation: {error}"))
}
