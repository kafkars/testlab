//! Assigned-record transfer validation joins live owners to one source operation.

#[cfg(test)]
#[path = "assigned_record_transfer_validation_test.rs"]
mod tests;

use crate::AssignedRecordTransferAction;

pub(crate) fn validate(
    action: &AssignedRecordTransferAction,
    state: &mut crate::scenario_action_validation::ActionStates,
    problems: &mut Vec<String>,
) {
    crate::scenario_action_validation::require_open_producer(
        &action.producer_id,
        &state.producers,
        problems,
    );
    crate::receive_action_validation::validate(
        &action.consumer_id,
        &action.operation_id,
        &action.expected_input_operation_id,
        action.timeout_ms,
        &mut state.consumers,
        &mut (&mut state.operation_ids, &state.sends),
        problems,
    );
    state.sends.insert(action.operation_id.clone());
    if action.target_topic.is_empty() || action.target_topic.len() > 249 {
        problems.push(format!(
            "assigned-record transfer {} has invalid target topic",
            action.operation_id
        ));
    }
    if action.target_partition < 0 {
        problems.push(format!(
            "assigned-record transfer {} has negative target partition {}",
            action.operation_id, action.target_partition
        ));
    }
}
