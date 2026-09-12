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
mod tests {
    use super::*;
    use crate::RecordSpec;

    #[test]
    fn admin_partition_abort_requires_one_record() {
        let transaction_id = id();
        let mut problems = Vec::new();
        execute(
            &transaction_id,
            &[],
            TransactionDisposition::AdminPartitionAbort,
            &mut problems,
        );
        assert_eq!(problems.len(), 1);
        problems.clear();
        execute(
            &transaction_id,
            &[record("one"), record("two")],
            TransactionDisposition::AdminPartitionAbort,
            &mut problems,
        );
        assert_eq!(problems.len(), 1);
        problems.clear();
        execute(
            &transaction_id,
            &[record("one")],
            TransactionDisposition::AdminPartitionAbort,
            &mut problems,
        );
        assert!(problems.is_empty());
    }

    #[test]
    fn transactional_transform_rejects_admin_partition_abort() {
        let mut problems = Vec::new();
        transform(
            &id(),
            TransactionDisposition::AdminPartitionAbort,
            &mut problems,
        );
        assert_eq!(problems.len(), 1);
        problems.clear();
        transform(&id(), TransactionDisposition::Abort, &mut problems);
        assert!(problems.is_empty());
    }

    fn id() -> OperationId {
        OperationId::new("admin-abort").unwrap_or_else(|error| panic!("operation ID: {error}"))
    }

    fn record(id: &str) -> BatchRecord {
        BatchRecord {
            operation_id: OperationId::new(id)
                .unwrap_or_else(|error| panic!("operation ID: {error}")),
            record: RecordSpec {
                topic: "orders".to_owned(),
                partition: 0,
                sequence: 1,
                key: None,
                value: None,
                headers: Vec::new(),
                timestamp_millis: None,
            },
        }
    }
}
