//! Admin partition-abort validation keeps its narrow public shape explicit.

use crate::{BatchRecord, OperationId, TransactionDisposition};

pub(crate) fn execute(
    transaction_id: &OperationId,
    operations: &[BatchRecord],
    disposition: TransactionDisposition,
    problems: &mut Vec<String>,
) {
    if disposition == TransactionDisposition::AdminPartitionAbort && operations.len() != 1 {
        problems.push(format!(
            "admin partition abort {transaction_id} requires exactly one staged record"
        ));
    }
}

pub(crate) fn transform(
    transaction_id: &OperationId,
    disposition: TransactionDisposition,
    problems: &mut Vec<String>,
) {
    if disposition == TransactionDisposition::AdminPartitionAbort {
        problems.push(format!(
            "transactional transform {transaction_id} cannot use admin_partition_abort"
        ));
    }
}

#[cfg(test)]
#[path = "transaction_abort_validation_test.rs"]
mod tests;
