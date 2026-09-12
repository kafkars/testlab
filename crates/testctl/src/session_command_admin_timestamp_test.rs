//! Timestamp offset translation preserves the selector without verifier expectations.

use testlab_schema::{
    AdapterCommand, AdminOffsetSelector, ClientId, ListOffsetsAction, ListOffsetsCommand,
    OperationId, ScenarioAction,
};

#[test]
fn timestamp_offset_translation_preserves_the_exact_public_selector() {
    let client_id = super::id(ClientId::new("client-1"));
    let operation_id = super::id(OperationId::new("admin-offset-timestamp"));
    let action = ScenarioAction::ListOffsets(ListOffsetsAction {
        client_id: client_id.clone(),
        operation_id: operation_id.clone(),
        topic: "orders".to_owned(),
        partition: 0,
        position: AdminOffsetSelector::Timestamp,
        timestamp_millis: Some(1_700_000_000_123),
        expected_offset: Some(1),
        expected_error_code: None,
        timeout_ms: 20_000,
    });

    let Some((command, _)) = super::translate(&action) else {
        panic!("timestamp offset listing must cross the adapter boundary");
    };
    assert_eq!(
        command,
        AdapterCommand::ListOffsets(ListOffsetsCommand {
            client_id,
            operation_id,
            topic: "orders".to_owned(),
            partition: 0,
            position: AdminOffsetSelector::Timestamp,
            timestamp_millis: Some(1_700_000_000_123),
            timeout_ms: 20_000,
        })
    );
}

#[test]
fn max_timestamp_translation_preserves_the_exact_public_selector() {
    let client_id = super::id(ClientId::new("client-1"));
    let operation_id = super::id(OperationId::new("admin-offset-max-timestamp"));
    let action = ScenarioAction::ListOffsets(ListOffsetsAction {
        client_id: client_id.clone(),
        operation_id: operation_id.clone(),
        topic: "orders".to_owned(),
        partition: 0,
        position: AdminOffsetSelector::MaxTimestamp,
        timestamp_millis: None,
        expected_offset: Some(0),
        expected_error_code: None,
        timeout_ms: 20_000,
    });

    let Some((command, _)) = super::translate(&action) else {
        panic!("max timestamp offset listing must cross the adapter boundary");
    };
    assert_eq!(
        command,
        AdapterCommand::ListOffsets(ListOffsetsCommand {
            client_id,
            operation_id,
            topic: "orders".to_owned(),
            partition: 0,
            position: AdminOffsetSelector::MaxTimestamp,
            timestamp_millis: None,
            timeout_ms: 20_000,
        })
    );
}
