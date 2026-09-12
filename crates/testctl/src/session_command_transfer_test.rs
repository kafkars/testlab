//! Owned transfer command translation keeps source expectations out of the adapter.

use testlab_schema::{
    AdapterCommand, AdapterEvent, AssignedRecordConversionMethod, AssignedRecordTransferAction,
    AssignedRecordTransferCompletion, ConsumedRecord, ConsumerId, OperationId, ProducerId,
    ScenarioAction, TerminalStatus,
};

use crate::runner_protocol::{EventDisposition, ExpectedEvent};

#[test]
fn transfer_translation_retains_only_public_inputs() {
    let action = ScenarioAction::TransferAssignedRecord(AssignedRecordTransferAction {
        consumer_id: id(ConsumerId::new("consumer-1")),
        producer_id: id(ProducerId::new("producer-1")),
        operation_id: id(OperationId::new("transfer-1")),
        expected_input_operation_id: id(OperationId::new("source-1")),
        method: AssignedRecordConversionMethod::IntoOwnedRecords,
        target_topic: "destination".to_owned(),
        target_partition: 2,
        timeout_ms: 30_000,
    });
    let Some((AdapterCommand::TransferAssignedRecord(command), expected)) =
        crate::session_command_consumer::translate(&action)
    else {
        panic!("owned transfer must translate");
    };
    assert_eq!(command.operation_id.as_str(), "transfer-1");
    assert_eq!(command.target_topic, "destination");
    assert_eq!(command.target_partition, 2);
    assert_eq!(
        command.method,
        AssignedRecordConversionMethod::IntoOwnedRecords
    );
    assert!(matches!(
        expected,
        ExpectedEvent::AssignedRecordTransferCompleted(operation)
            if operation.as_str() == "transfer-1"
    ));
}

#[test]
fn transfer_admission_and_terminal_are_intermediate_until_completion() {
    let operation_id = id(OperationId::new("transfer-1"));
    let expected = ExpectedEvent::AssignedRecordTransferCompleted(operation_id.clone());
    let accepted = AdapterEvent::OperationAccepted {
        operation_id: operation_id.clone(),
    };
    let terminal = AdapterEvent::OperationTerminal {
        operation_id: operation_id.clone(),
        status: TerminalStatus::Acknowledged,
        code: None,
        partition: Some(1),
        offset: Some(2),
        timestamp_millis: None,
        receipt: None,
    };
    assert_eq!(
        expected.classify(&accepted).unwrap_or_else(classify_error),
        EventDisposition::Continue
    );
    assert_eq!(
        expected.classify(&terminal).unwrap_or_else(classify_error),
        EventDisposition::Continue
    );
    let record = ConsumedRecord {
        topic: "source".to_owned(),
        partition: 0,
        offset: 0,
        timestamp_millis: None,
        key: None,
        value: None,
        headers: Vec::new(),
    };
    let completion =
        AdapterEvent::AssignedRecordTransferCompleted(AssignedRecordTransferCompletion {
            operation_id,
            source_record: record.clone(),
            retained_source_record: record,
            status: TerminalStatus::Acknowledged,
            code: None,
            partition: Some(1),
            offset: Some(2),
            timestamp_millis: None,
            receipt: None,
        });
    assert_eq!(
        expected
            .classify(&completion)
            .unwrap_or_else(classify_error),
        EventDisposition::Complete
    );
}

fn classify_error(error: crate::run_error::RunFailure) -> ! {
    panic!("classify owned transfer event: {error}")
}

fn id<T, E>(result: Result<T, E>) -> T
where
    E: std::fmt::Display,
{
    result.unwrap_or_else(|error| panic!("fixture identity: {error}"))
}
