//! Batched record-deletion protocol pins private baselines and caller order.

use testlab_schema::{
    AdapterCommand, AdapterEvent, AdminRecordsBatchDeleted, AdminRecordsDeletionOutcome, ClientId,
    DeleteRecordsBatchAction, DeleteRecordsBatchExpectation, DeleteRecordsBoundary, OperationId,
    ScenarioAction,
};

use crate::runner_protocol::{EventDisposition, ExpectedEvent};

#[test]
fn translation_preserves_boundaries_and_hides_expected_watermarks() {
    let action = ScenarioAction::DeleteRecordsBatch(action());
    let Some((AdapterCommand::DeleteRecordsBatch(command), expected)) =
        crate::session_command_admin_records::translate(&action)
    else {
        panic!("batched record-deletion translation");
    };
    assert_eq!(command.client_id, client());
    assert_eq!(command.operation_id, operation());
    assert_eq!(command.timeout_ms, 1_000);
    assert_eq!(command.targets.len(), 2);
    assert_eq!(command.targets[0].partition, 1);
    assert_eq!(
        command.targets[0].boundary,
        DeleteRecordsBoundary::Offset { offset: 2 }
    );
    assert_eq!(command.targets[1].partition, 0);
    assert_eq!(
        command.targets[1].boundary,
        DeleteRecordsBoundary::HighWatermark
    );
    let encoded = serde_json::to_string(&command)
        .unwrap_or_else(|error| panic!("encode batched record deletion: {error}"));
    assert!(!encoded.contains("expected_high_watermark"));
    assert!(matches!(
        expected,
        ExpectedEvent::RecordsBatchDeleted { .. }
    ));
}

#[test]
fn completion_requires_every_target_identity_in_caller_order() {
    let expected = ExpectedEvent::RecordsBatchDeleted {
        operation_id: operation(),
        targets: targets()
            .iter()
            .map(|target| (target.topic.clone(), target.partition))
            .collect(),
    };
    let mut event = AdapterEvent::RecordsBatchDeleted(completion());
    assert_eq!(
        expected
            .classify(&event)
            .unwrap_or_else(|error| panic!("classify batched record deletion: {error}")),
        EventDisposition::Complete
    );
    let AdapterEvent::RecordsBatchDeleted(actual) = &mut event else {
        panic!("batched record-deletion event");
    };
    actual.outcomes.swap(0, 1);
    let error = expected
        .classify(&event)
        .err()
        .unwrap_or_else(|| panic!("reordered outcomes must not complete"));
    assert_eq!(error.harness_error().code, "event_identity_mismatch");
}

fn action() -> DeleteRecordsBatchAction {
    DeleteRecordsBatchAction {
        client_id: client(),
        operation_id: operation(),
        targets: targets(),
        timeout_ms: 1_000,
    }
}

fn targets() -> Vec<DeleteRecordsBatchExpectation> {
    vec![
        target(1, DeleteRecordsBoundary::Offset { offset: 2 }, 3),
        target(0, DeleteRecordsBoundary::HighWatermark, 2),
    ]
}

fn target(
    partition: i32,
    boundary: DeleteRecordsBoundary,
    expected_high_watermark: i64,
) -> DeleteRecordsBatchExpectation {
    DeleteRecordsBatchExpectation {
        topic: "records".to_owned(),
        partition,
        boundary,
        expected_high_watermark,
    }
}

fn completion() -> AdminRecordsBatchDeleted {
    AdminRecordsBatchDeleted {
        operation_id: operation(),
        outcomes: vec![outcome(1, 2), outcome(0, 2)],
    }
}

fn outcome(partition: i32, low_watermark: i64) -> AdminRecordsDeletionOutcome {
    AdminRecordsDeletionOutcome {
        topic: "records".to_owned(),
        partition,
        low_watermark: Some(low_watermark),
        error_code: None,
    }
}

fn client() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client: {error}"))
}

fn operation() -> OperationId {
    OperationId::new("delete-records-batch").unwrap_or_else(|error| panic!("operation: {error}"))
}
