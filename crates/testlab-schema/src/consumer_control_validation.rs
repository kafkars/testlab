//! Consumer control routing keeps assigned, hosted, and shutdown validation separate.

use crate::ScenarioAction;
use crate::scenario_action_validation::ActionStates;

pub(crate) fn validate(
    action: &ScenarioAction,
    state: &mut ActionStates,
    problems: &mut Vec<String>,
) {
    match action {
        ScenarioAction::ControlAssignedConsumer(action) => {
            crate::assigned_consumer_control_validation::validate(
                action,
                &mut state.consumers,
                &mut state.operation_ids,
                problems,
            );
        }
        ScenarioAction::ControlGroupConsumer(action) => {
            crate::group_consumer_control_validation::validate(
                action,
                &mut state.consumers,
                &mut state.operation_ids,
                problems,
            );
        }
        ScenarioAction::ShutdownGroupConsumer(action) => {
            crate::group_consumer_shutdown_validation::validate(
                action,
                &mut state.consumers,
                &mut state.operation_ids,
                problems,
            );
        }
        _ => unreachable!("non-control action reached consumer control validation"),
    }
}
