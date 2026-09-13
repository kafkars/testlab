//! Transaction protocol tests pin administrative-abort producer snapshots.

use std::collections::BTreeSet;

use testlab_schema::{
    AdapterEvent, AdminProducersDescription, OperationId, TransactionDisposition,
};

use crate::runner_protocol::{EventDisposition, ExpectedEvent};
use crate::runner_protocol_transaction::{TransactionExpectation, classify};

#[test]
fn admin_abort_accepts_only_its_two_producer_snapshot_identities() {
    let transaction_id = operation("transaction-1");
    let record_id = operation("record-1");
    let expected = ExpectedEvent::TransactionCompleted(TransactionExpectation {
        transaction_id: transaction_id.clone(),
        operation_ids: BTreeSet::from([record_id.clone()]),
        disposition: TransactionDisposition::AdminPartitionAbort,
    });
    for operation_id in [record_id, transaction_id] {
        assert_eq!(
            classify(&expected, &description(operation_id))
                .unwrap_or_else(|| panic!("producer snapshot classification"))
                .unwrap_or_else(|error| panic!("producer snapshot identity: {error}")),
            EventDisposition::Continue
        );
    }
    assert!(
        classify(&expected, &description(operation("wrong")))
            .unwrap_or_else(|| panic!("producer snapshot classification"))
            .is_err()
    );
}

fn description(operation_id: OperationId) -> AdapterEvent {
    AdapterEvent::ProducersDescribed(AdminProducersDescription {
        operation_id,
        topic: "orders".to_owned(),
        partition: 0,
        producers: Vec::new(),
    })
}

fn operation(value: &str) -> OperationId {
    OperationId::new(value).unwrap_or_else(|error| panic!("operation ID: {error}"))
}
