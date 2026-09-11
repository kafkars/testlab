//! Share-group command matching keeps scenario-only expectations off the wire.

use testlab_schema::{AdapterCommand, OperationId, ScenarioAction};

pub(super) fn action_operation_id(action: &ScenarioAction) -> Option<&OperationId> {
    let ScenarioAction::DescribeShareGroup(value) = action else {
        return None;
    };
    Some(&value.operation_id)
}

pub(super) fn command_operation_id(command: &AdapterCommand) -> Option<&OperationId> {
    let AdapterCommand::DescribeShareGroup(value) = command else {
        return None;
    };
    Some(&value.operation_id)
}

pub(super) fn matches(action: &ScenarioAction, command: &AdapterCommand) -> Option<bool> {
    let matches = match (action, command) {
        (
            ScenarioAction::DescribeShareGroup(action),
            AdapterCommand::DescribeShareGroup(command),
        ) => {
            action.client_id == command.client_id
                && action.operation_id == command.operation_id
                && action.group_id == command.group_id
                && action.timeout_ms == command.timeout_ms
        }
        (ScenarioAction::DescribeShareGroup(_), _) | (_, AdapterCommand::DescribeShareGroup(_)) => {
            false
        }
        _ => return None,
    };
    Some(matches)
}
