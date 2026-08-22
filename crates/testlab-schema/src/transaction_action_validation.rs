//! Transaction validation owns linear producer handles and staged record identities.

use std::collections::BTreeMap;

use crate::scenario_action_validation::ActionStates;
use crate::{ClientId, OperationId, ProducerId, ScenarioAction, TransactionDisposition};

pub(crate) type TransactionStates = BTreeMap<ProducerId, TransactionState>;
pub(crate) type TransactionSends = BTreeMap<OperationId, TransactionRecordOutcome>;
const MAX_TRANSACTION_RECORDS: usize = 31;

pub(crate) struct TransactionState {
    client_id: ClientId,
    transactional_id: String,
    closed: bool,
}

#[derive(Clone, Copy)]
pub(crate) enum TransactionRecordOutcome {
    Completed(TransactionDisposition),
    Fenced,
}

pub(crate) fn validate(
    action: &ScenarioAction,
    state: &mut ActionStates,
    problems: &mut Vec<String>,
) {
    match action {
        ScenarioAction::CreateTransactionalProducer {
            client_id,
            producer_id,
            transactional_id,
            transaction_timeout_ms,
            initialization_timeout_ms,
        } => create(
            client_id,
            producer_id,
            transactional_id,
            *transaction_timeout_ms,
            *initialization_timeout_ms,
            state,
            problems,
        ),
        ScenarioAction::ExecuteTransaction {
            producer_id,
            transaction_id,
            operations,
            disposition,
            timeout_ms,
        } => execute(
            producer_id,
            transaction_id,
            operations,
            *disposition,
            *timeout_ms,
            state,
            problems,
        ),
        ScenarioAction::FenceTransaction {
            producer_id,
            transaction_id,
            operation,
            replacement_client_id,
            replacement_producer_id,
            transactional_id,
            transaction_timeout_ms,
            initialization_timeout_ms,
            timeout_ms,
        } => fence(
            producer_id,
            transaction_id,
            operation,
            replacement_client_id,
            replacement_producer_id,
            transactional_id,
            *transaction_timeout_ms,
            *initialization_timeout_ms,
            *timeout_ms,
            state,
            problems,
        ),
        ScenarioAction::CloseTransactionalProducer { producer_id } => {
            close(producer_id, &mut state.transactions, problems);
        }
        _ => {}
    }
}

fn create(
    client_id: &ClientId,
    producer_id: &ProducerId,
    transactional_id: &str,
    transaction_timeout_ms: u64,
    initialization_timeout_ms: u64,
    state: &mut ActionStates,
    problems: &mut Vec<String>,
) {
    match state.clients.get(client_id) {
        Some(false) => {}
        Some(true) => problems.push(format!(
            "transactional producer {producer_id} uses shut down client {client_id}"
        )),
        None => problems.push(format!(
            "transactional producer {producer_id} uses missing client {client_id}"
        )),
    }
    if state.producers.contains_key(producer_id)
        || state
            .transactions
            .insert(
                producer_id.clone(),
                TransactionState {
                    client_id: client_id.clone(),
                    transactional_id: transactional_id.to_owned(),
                    closed: false,
                },
            )
            .is_some()
    {
        problems.push(format!("duplicate producer id {producer_id}"));
    }
    if transactional_id.is_empty() || transactional_id.len() > 249 {
        problems.push(format!(
            "transactional producer {producer_id} has invalid transactional_id"
        ));
    }
    validate_timeout(
        producer_id,
        "transaction_timeout_ms",
        transaction_timeout_ms,
        problems,
    );
    validate_timeout(
        producer_id,
        "initialization_timeout_ms",
        initialization_timeout_ms,
        problems,
    );
}

fn execute(
    producer_id: &ProducerId,
    transaction_id: &OperationId,
    operations: &[crate::BatchRecord],
    disposition: TransactionDisposition,
    timeout_ms: u64,
    state: &mut ActionStates,
    problems: &mut Vec<String>,
) {
    require_open(producer_id, &state.transactions, problems);
    if !state.operation_ids.insert(transaction_id.clone()) {
        problems.push(format!("duplicate operation id {transaction_id}"));
    }
    if operations.is_empty() || operations.len() > MAX_TRANSACTION_RECORDS {
        problems.push(format!(
            "transaction {transaction_id} must contain between 1 and {MAX_TRANSACTION_RECORDS} records"
        ));
    }
    for operation in operations {
        record_operation(
            operation,
            TransactionRecordOutcome::Completed(disposition),
            state,
            problems,
        );
    }
    validate_timeout(producer_id, "timeout_ms", timeout_ms, problems);
}

#[allow(
    clippy::too_many_arguments,
    reason = "the validator mirrors every explicit fencing manifest field"
)]
fn fence(
    producer_id: &ProducerId,
    transaction_id: &OperationId,
    operation: &crate::BatchRecord,
    replacement_client_id: &ClientId,
    replacement_producer_id: &ProducerId,
    transactional_id: &str,
    transaction_timeout_ms: u64,
    initialization_timeout_ms: u64,
    timeout_ms: u64,
    state: &mut ActionStates,
    problems: &mut Vec<String>,
) {
    require_open(producer_id, &state.transactions, problems);
    if state
        .transactions
        .get(producer_id)
        .is_some_and(|owner| owner.transactional_id != transactional_id)
    {
        problems.push(format!(
            "fence transaction {transaction_id} must reuse producer {producer_id} transactional_id"
        ));
    }
    if !state.operation_ids.insert(transaction_id.clone()) {
        problems.push(format!("duplicate operation id {transaction_id}"));
    }
    record_operation(operation, TransactionRecordOutcome::Fenced, state, problems);
    create(
        replacement_client_id,
        replacement_producer_id,
        transactional_id,
        transaction_timeout_ms,
        initialization_timeout_ms,
        state,
        problems,
    );
    validate_timeout(producer_id, "timeout_ms", timeout_ms, problems);
}

fn record_operation(
    operation: &crate::BatchRecord,
    outcome: TransactionRecordOutcome,
    state: &mut ActionStates,
    problems: &mut Vec<String>,
) {
    if !state.operation_ids.insert(operation.operation_id.clone()) {
        problems.push(format!("duplicate operation id {}", operation.operation_id));
    }
    state.sends.insert(operation.operation_id.clone());
    state
        .transaction_sends
        .insert(operation.operation_id.clone(), outcome);
    if let Err(error) = operation.record.validate() {
        problems.push(format!(
            "operation {} has invalid record: {error}",
            operation.operation_id
        ));
    }
}

fn require_open(
    producer_id: &ProducerId,
    transactions: &TransactionStates,
    problems: &mut Vec<String>,
) {
    match transactions.get(producer_id) {
        Some(owner) if !owner.closed => {}
        Some(_) => problems.push(format!(
            "transactional producer {producer_id} was used after close"
        )),
        None => problems.push(format!(
            "missing transactional producer {producer_id} was used"
        )),
    }
}

fn close(
    producer_id: &ProducerId,
    transactions: &mut TransactionStates,
    problems: &mut Vec<String>,
) {
    match transactions.get_mut(producer_id) {
        Some(owner) if !owner.closed => owner.closed = true,
        Some(_) => problems.push(format!(
            "transactional producer {producer_id} closed more than once"
        )),
        None => problems.push(format!(
            "missing transactional producer {producer_id} was closed"
        )),
    }
}

fn validate_timeout(
    producer_id: &ProducerId,
    name: &str,
    timeout_ms: u64,
    problems: &mut Vec<String>,
) {
    if !(100..=600_000).contains(&timeout_ms) {
        problems.push(format!(
            "transactional producer {producer_id} {name} must be between 100 and 600000"
        ));
    }
}

pub(crate) fn open_for_client(
    transactions: &TransactionStates,
    client_id: &ClientId,
) -> Vec<String> {
    transactions
        .iter()
        .filter(|(_, owner)| &owner.client_id == client_id && !owner.closed)
        .map(|(producer, _)| producer.to_string())
        .collect()
}

pub(crate) fn unclosed(transactions: TransactionStates) -> Vec<ProducerId> {
    transactions
        .into_iter()
        .filter_map(|(producer, owner)| (!owner.closed).then_some(producer))
        .collect()
}
