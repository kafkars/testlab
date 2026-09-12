//! Ordinary transaction validation owns record identities and exact staging selection.

use crate::scenario_action_validation::ActionStates;
use crate::{BatchRecord, OperationId, ProducerId, TransactionDisposition, TransactionSendMethod};

use super::{MAX_TRANSACTION_RECORDS, TransactionRecordOutcome};

#[allow(
    clippy::too_many_arguments,
    reason = "the validator mirrors the exact transaction command"
)]
pub(super) fn execute(
    producer_id: &ProducerId,
    transaction_id: &OperationId,
    operations: &[BatchRecord],
    method: TransactionSendMethod,
    disposition: TransactionDisposition,
    timeout_ms: u64,
    state: &mut ActionStates,
    problems: &mut Vec<String>,
) {
    super::require_open(producer_id, &state.transactions, problems);
    crate::transaction_abort_validation::execute(transaction_id, operations, disposition, problems);
    super::batch_send_validation::validate(transaction_id, method, operations, problems);
    if !state.operation_ids.insert(transaction_id.clone()) {
        problems.push(format!("duplicate operation id {transaction_id}"));
    }
    if operations.is_empty() || operations.len() > MAX_TRANSACTION_RECORDS {
        problems.push(format!(
            "transaction {transaction_id} must contain between 1 and {MAX_TRANSACTION_RECORDS} records"
        ));
    }
    for operation in operations {
        super::record_operation(
            operation,
            TransactionRecordOutcome::Completed(disposition),
            state,
            problems,
        );
    }
    super::validate_timeout(producer_id, "timeout_ms", timeout_ms, problems);
}
