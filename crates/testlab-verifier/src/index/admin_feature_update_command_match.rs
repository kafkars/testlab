//! Validation-only feature updates retain exact action and wire identity.

use testlab_schema::{AdapterCommand, OperationId, ScenarioAction};

pub(super) fn action_operation_id(action: &ScenarioAction) -> Option<&OperationId> {
    match action {
        ScenarioAction::ValidateFeatureUpdates(value) => Some(&value.operation_id),
        _ => None,
    }
}

pub(super) fn command_operation_id(command: &AdapterCommand) -> Option<&OperationId> {
    match command {
        AdapterCommand::ValidateFeatureUpdates(value) => Some(&value.operation_id),
        _ => None,
    }
}

pub(super) fn matches(action: &ScenarioAction, command: &AdapterCommand) -> Option<bool> {
    Some(match (action, command) {
        (
            ScenarioAction::ValidateFeatureUpdates(action),
            AdapterCommand::ValidateFeatureUpdates(command),
        ) => {
            action.client_id == command.client_id
                && action.operation_id == command.operation_id
                && action.updates == command.updates
                && action.timeout_ms == command.timeout_ms
        }
        (ScenarioAction::ValidateFeatureUpdates(_), _)
        | (_, AdapterCommand::ValidateFeatureUpdates(_)) => false,
        _ => return None,
    })
}
