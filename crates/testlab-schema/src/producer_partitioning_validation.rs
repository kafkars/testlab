//! Ordinary send validation keeps partition selection beside record ownership.

use crate::ScenarioAction;
use crate::scenario_action_validation::{ActionStates, require_open_producer, validate_operation};

pub(crate) fn validate(
    action: &ScenarioAction,
    state: &mut ActionStates,
    problems: &mut Vec<String>,
) {
    let ScenarioAction::Send {
        producer_id,
        operation_id,
        partitioning,
        record,
        ..
    } = action
    else {
        unreachable!("producer partitioning validator requires one send");
    };
    require_open_producer(producer_id, &state.producers, problems);
    validate_operation(
        operation_id,
        record,
        &mut state.operation_ids,
        &mut state.sends,
        problems,
    );
    if let Err(error) = partitioning.expected_partition(record) {
        problems.push(format!(
            "operation {operation_id} has invalid producer partitioning: {error}"
        ));
    }
}
