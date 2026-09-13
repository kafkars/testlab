//! Singleton offset translation preserves the selector without verifier expectations.

use testlab_schema::{
    AdapterCommand, AdminOffsetSelector, AdminReadIsolation, ClientId, ListOffsetsAction,
    ListOffsetsCommand, OperationId, ScenarioAction,
};

#[test]
fn offset_translation_keeps_expected_result_private() {
    assert_translation(
        AdminOffsetSelector::Latest,
        AdminReadIsolation::ReadCommitted,
        2,
        42,
    );
}

#[test]
fn earliest_uncommitted_translation_preserves_both_public_selectors() {
    assert_translation(
        AdminOffsetSelector::Earliest,
        AdminReadIsolation::ReadUncommitted,
        0,
        0,
    );
}

fn assert_translation(
    position: AdminOffsetSelector,
    read_isolation: AdminReadIsolation,
    partition: i32,
    expected_offset: i64,
) {
    let client_id = super::id(ClientId::new("client-1"));
    let operation_id = super::id(OperationId::new("admin-list-offsets-1"));
    let action = ScenarioAction::ListOffsets(ListOffsetsAction {
        client_id: client_id.clone(),
        operation_id: operation_id.clone(),
        topic: "orders".to_owned(),
        partition,
        position,
        read_isolation,
        timestamp_millis: None,
        expected_offset: Some(expected_offset),
        expected_error_code: None,
        timeout_ms: 20_000,
    });

    let Some((command, _)) = super::translate(&action) else {
        panic!("offset listing must cross the adapter boundary");
    };
    assert_eq!(
        command,
        AdapterCommand::ListOffsets(ListOffsetsCommand {
            client_id,
            operation_id,
            topic: "orders".to_owned(),
            partition,
            position,
            read_isolation,
            timestamp_millis: None,
            timeout_ms: 20_000,
        })
    );
}
