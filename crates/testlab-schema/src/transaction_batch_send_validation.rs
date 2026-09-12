//! Transactional batch validation preserves the public homogeneous-batch contract.

use crate::{BatchRecord, OperationId, TransactionSendMethod};

pub(super) fn validate(
    transaction_id: &OperationId,
    method: TransactionSendMethod,
    operations: &[BatchRecord],
    problems: &mut Vec<String>,
) {
    if method != TransactionSendMethod::SendBatch {
        return;
    }
    if operations.len() < 2 {
        problems.push(format!(
            "transaction {transaction_id} send_batch requires at least 2 records"
        ));
    }
    let Some(first) = operations.first() else {
        return;
    };
    if operations.iter().any(|operation| {
        operation.record.topic != first.record.topic
            || operation.record.partition != first.record.partition
    }) {
        problems.push(format!(
            "transaction {transaction_id} send_batch records must share one topic and partition"
        ));
    }
}
