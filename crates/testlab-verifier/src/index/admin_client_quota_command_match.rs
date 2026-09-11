//! Client-quota command matching binds exact scenario intent to one wire request.

use testlab_schema::{AdapterCommand, OperationId, ScenarioAction};

pub(super) fn action_operation_id(action: &ScenarioAction) -> Option<&OperationId> {
    match action {
        ScenarioAction::AlterClientQuota(value) => Some(&value.operation_id),
        ScenarioAction::DescribeClientQuota(value) => Some(&value.operation_id),
        _ => None,
    }
}

pub(super) fn command_operation_id(command: &AdapterCommand) -> Option<&OperationId> {
    match command {
        AdapterCommand::AlterClientQuota(value) => Some(&value.operation_id),
        AdapterCommand::DescribeClientQuota(value) => Some(&value.operation_id),
        _ => None,
    }
}

pub(super) fn matches(action: &ScenarioAction, command: &AdapterCommand) -> Option<bool> {
    let matches = match (action, command) {
        (ScenarioAction::AlterClientQuota(action), AdapterCommand::AlterClientQuota(command)) => {
            action == command
        }
        (
            ScenarioAction::DescribeClientQuota(action),
            AdapterCommand::DescribeClientQuota(command),
        ) => {
            action.client_id == command.client_id
                && action.operation_id == command.operation_id
                && action.user == command.user
                && action.direction == command.direction
                && action.timeout_ms == command.timeout_ms
        }
        (ScenarioAction::AlterClientQuota(_) | ScenarioAction::DescribeClientQuota(_), _)
        | (_, AdapterCommand::AlterClientQuota(_) | AdapterCommand::DescribeClientQuota(_)) => {
            false
        }
        _ => return None,
    };
    Some(matches)
}
