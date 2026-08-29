//! Producer cancellation validation reuses ordinary ownership and operation rules.

use crate::scenario_action_validation::{ActionStates, require_open_producer, validate_operation};

pub(crate) fn validate(
    action: &crate::CancelProducerSendCommand,
    state: &mut ActionStates,
    problems: &mut Vec<String>,
) {
    require_open_producer(&action.producer_id, &state.producers, problems);
    validate_operation(
        &action.operation_id,
        &action.record,
        &mut state.operation_ids,
        &mut state.sends,
        problems,
    );
    if !(100..=60_000).contains(&action.timeout_ms) {
        problems.push(format!(
            "cancel producer send {} timeout_ms must be between 100 and 60000",
            action.operation_id
        ));
    }
}
