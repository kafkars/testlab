//! Transactional-transform validation binds public group and transactional owners.

use crate::scenario_action_validation::ActionStates;
use crate::transaction_action_validation::{
    MAX_TRANSACTION_RECORDS, TransactionRecordOutcome, record_operation, require_open,
    validate_timeout,
};

pub(crate) fn validate(
    action: &crate::TransactionalTransformAction,
    state: &mut ActionStates,
    problems: &mut Vec<String>,
) {
    require_open(&action.producer_id, &state.transactions, problems);
    match state.consumers.get(&action.consumer_id) {
        Some(consumer) if !consumer.closed && consumer.group.is_some() => {}
        Some(consumer) if consumer.closed => problems.push(format!(
            "transactional transform {} uses closed consumer {}",
            action.transaction_id, action.consumer_id
        )),
        Some(_) => problems.push(format!(
            "transactional transform {} requires a group consumer",
            action.transaction_id
        )),
        None => problems.push(format!(
            "transactional transform {} uses missing consumer {}",
            action.transaction_id, action.consumer_id
        )),
    }
    if !state.sends.contains(&action.expected_input_operation_id) {
        problems.push(format!(
            "transactional transform {} expects missing prior send {}",
            action.transaction_id, action.expected_input_operation_id
        ));
    }
    if !state.operation_ids.insert(action.transaction_id.clone()) {
        problems.push(format!("duplicate operation id {}", action.transaction_id));
    }
    if action.operations.is_empty() || action.operations.len() > MAX_TRANSACTION_RECORDS {
        problems.push(format!(
            "transactional transform {} must contain between 1 and {MAX_TRANSACTION_RECORDS} output records",
            action.transaction_id
        ));
    }
    for operation in &action.operations {
        record_operation(
            operation,
            TransactionRecordOutcome::Completed(action.disposition),
            state,
            problems,
        );
    }
    validate_timeout(
        &action.producer_id,
        "timeout_ms",
        action.timeout_ms,
        problems,
    );
}
