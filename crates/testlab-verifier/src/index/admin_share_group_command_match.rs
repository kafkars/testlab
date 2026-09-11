//! Share-group command matching keeps scenario-only expectations off the wire.

use testlab_schema::{AdapterCommand, OperationId, ScenarioAction};

pub(super) fn action_operation_id(action: &ScenarioAction) -> Option<&OperationId> {
    match action {
        ScenarioAction::DescribeShareGroup(value) => Some(&value.operation_id),
        ScenarioAction::ListShareGroupOffsets(value) => Some(&value.operation_id),
        ScenarioAction::AlterShareGroupOffsets(value) => Some(&value.operation_id),
        ScenarioAction::DeleteShareGroupOffsets(value) => Some(&value.operation_id),
        _ => None,
    }
}

pub(super) fn command_operation_id(command: &AdapterCommand) -> Option<&OperationId> {
    match command {
        AdapterCommand::DescribeShareGroup(value) => Some(&value.operation_id),
        AdapterCommand::ListShareGroupOffsets(value) => Some(&value.operation_id),
        AdapterCommand::AlterShareGroupOffsets(value) => Some(&value.operation_id),
        AdapterCommand::DeleteShareGroupOffsets(value) => Some(&value.operation_id),
        _ => None,
    }
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
        (
            ScenarioAction::ListShareGroupOffsets(action),
            AdapterCommand::ListShareGroupOffsets(command),
        ) => {
            action.client_id == command.client_id
                && action.operation_id == command.operation_id
                && action.group_id == command.group_id
                && action.topic == command.topic
                && action.partition == command.partition
                && action.timeout_ms == command.timeout_ms
        }
        (ScenarioAction::ListShareGroupOffsets(_), _)
        | (_, AdapterCommand::ListShareGroupOffsets(_)) => false,
        (
            ScenarioAction::AlterShareGroupOffsets(action),
            AdapterCommand::AlterShareGroupOffsets(command),
        ) => {
            action.client_id == command.client_id
                && action.operation_id == command.operation_id
                && action.group_id == command.group_id
                && action.topic == command.topic
                && action.partition == command.partition
                && action.start_offset == command.start_offset
                && action.timeout_ms == command.timeout_ms
        }
        (ScenarioAction::AlterShareGroupOffsets(_), _)
        | (_, AdapterCommand::AlterShareGroupOffsets(_)) => false,
        (
            ScenarioAction::DeleteShareGroupOffsets(action),
            AdapterCommand::DeleteShareGroupOffsets(command),
        ) => {
            action.client_id == command.client_id
                && action.operation_id == command.operation_id
                && action.group_id == command.group_id
                && action.topic == command.topic
                && action.timeout_ms == command.timeout_ms
        }
        (ScenarioAction::DeleteShareGroupOffsets(_), _)
        | (_, AdapterCommand::DeleteShareGroupOffsets(_)) => false,
        _ => return None,
    };
    Some(matches)
}
