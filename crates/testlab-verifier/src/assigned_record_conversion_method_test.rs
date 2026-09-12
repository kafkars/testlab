//! Owned-record conversion contracts preserve exact public method selection.

use testlab_schema::{
    AssignedRecordConversionMethod, AssignedRecordTransferAction, ConsumerId, OperationId,
    ProducerId,
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

fn id<T>(result: Result<T, impl std::fmt::Display>) -> T {
    result.unwrap_or_else(|error| panic!("fixture identity: {error}"))
}
