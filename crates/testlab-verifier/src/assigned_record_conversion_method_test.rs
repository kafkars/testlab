//! Owned-record conversion contracts preserve exact public method selection.

use testlab_schema::{
    AssignedRecordConversionMethod, AssignedRecordTransferAction, ByteString, ConsumerId,
    HeaderSpec, OperationId, ProducerId, RecordSpec,
};

#[test]
fn conversion_methods_select_distinct_contracts() {
    assert_eq!(
        super::contract(AssignedRecordConversionMethod::IntoOwnedBatch),
        "CONS-021"
    );
    assert_eq!(
        super::contract(AssignedRecordConversionMethod::IntoOwnedRecords),
        "CONS-023"
    );
}

#[test]
fn direct_conversion_survives_the_exact_adapter_command() {
    let action = AssignedRecordTransferAction {
        consumer_id: id(ConsumerId::new("consumer-1")),
        producer_id: id(ProducerId::new("producer-1")),
        operation_id: id(OperationId::new("transfer-1")),
        expected_input_operation_id: id(OperationId::new("source-1")),
        method: AssignedRecordConversionMethod::IntoOwnedRecords,
        target_topic: "destination".to_owned(),
        target_partition: 1,
        timeout_ms: 1_000,
    };

    assert_eq!(
        super::command(&action).method,
        AssignedRecordConversionMethod::IntoOwnedRecords
    );
}

#[test]
fn destination_record_bytes_compare_by_decoded_value() {
    let expected = RecordSpec {
        topic: "destination".to_owned(),
        partition: 1,
        sequence: 2,
        timestamp_millis: None,
        key: Some(ByteString::utf8("key")),
        value: Some(ByteString::utf8("value")),
        headers: vec![HeaderSpec {
            name: "header".to_owned(),
            value: Some(ByteString::utf8("value")),
        }],
    };
    let mut actual = expected.clone();
    actual.key = Some(ByteString::hex(b"key"));
    actual.value = Some(ByteString::hex(b"value"));
    actual.headers[0].value = Some(ByteString::hex(b"value"));

    assert!(super::exact_record_data(&actual, &expected));
}

fn id<T>(result: Result<T, impl std::fmt::Display>) -> T {
    result.unwrap_or_else(|error| panic!("fixture identity: {error}"))
}
