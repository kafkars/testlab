//! Transactional offset protocol tests pin composite completion identity.

use std::collections::BTreeSet;

use testlab_schema::{
    AdapterEvent, ConsumerId, GroupMembershipEpoch, OperationId, TransactionDisposition,
    TransactionalTransformCompletion,
};

use crate::runner_protocol::{EventDisposition, ExpectedEvent};

#[test]
fn transform_completion_requires_exact_transaction_identity() {
    let transaction_id = operation("transaction-1");
    let expected = ExpectedEvent::TransactionCompleted {
        transaction_id: transaction_id.clone(),
        operation_ids: BTreeSet::from([operation("output-1")]),
    };
    assert_eq!(
        expected
            .classify(&AdapterEvent::TransactionalTransformCompleted(completion(
                transaction_id,
            )))
            .unwrap_or_else(|error| panic!("classify transform completion: {error}")),
        EventDisposition::Complete
    );
    assert!(
        expected
            .classify(&AdapterEvent::TransactionalTransformCompleted(completion(
                operation("transaction-2"),
            )))
            .is_err()
    );
}

fn completion(transaction_id: OperationId) -> TransactionalTransformCompletion {
    TransactionalTransformCompletion {
        transaction_id,
        disposition: TransactionDisposition::Commit,
        consumer_id: ConsumerId::new("consumer-1")
            .unwrap_or_else(|error| panic!("consumer id: {error}")),
        records: Vec::new(),
        group_id: "group-1".to_owned(),
        topic: "input".to_owned(),
        partition: 0,
        next_offset: 1,
        group_epoch: GroupMembershipEpoch::Classic { generation_id: 1 },
    }
}

fn operation(value: &str) -> OperationId {
    OperationId::new(value).unwrap_or_else(|error| panic!("operation id: {error}"))
}
