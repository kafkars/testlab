//! Share-group command matching keeps scenario-only expectations off the wire.

use testlab_schema::{AdapterCommand, OperationId, ScenarioAction};

pub(super) fn action_operation_id(action: &ScenarioAction) -> Option<&OperationId> {
    match action {
        ScenarioAction::DescribeShareGroup(value) => Some(&value.operation_id),
        ScenarioAction::DescribeShareGroups(value) => Some(&value.operation_id),
        ScenarioAction::ListShareGroupOffsets(value) => Some(&value.operation_id),
        ScenarioAction::ListShareGroupsOffsets(value) => Some(&value.operation_id),
        ScenarioAction::AlterShareGroupOffsets(value) => Some(&value.operation_id),
        ScenarioAction::DeleteShareGroupOffsets(value) => Some(&value.operation_id),
        ScenarioAction::DeleteShareGroups(value) => Some(&value.operation_id),
        _ => None,
    }
}

pub(super) fn command_operation_id(command: &AdapterCommand) -> Option<&OperationId> {
    match command {
        AdapterCommand::DescribeShareGroup(value) => Some(&value.operation_id),
        AdapterCommand::DescribeShareGroups(value) => Some(&value.operation_id),
        AdapterCommand::ListShareGroupOffsets(value) => Some(&value.operation_id),
        AdapterCommand::ListShareGroupsOffsets(value) => Some(&value.operation_id),
        AdapterCommand::AlterShareGroupOffsets(value) => Some(&value.operation_id),
        AdapterCommand::DeleteShareGroupOffsets(value) => Some(&value.operation_id),
        AdapterCommand::DeleteShareGroups(value) => Some(&value.operation_id),
        _ => None,
    }
}

#[allow(
    clippy::too_many_lines,
    reason = "exact Share-group matching keeps every public command field explicit"
)]
pub(super) fn matches(action: &ScenarioAction, command: &AdapterCommand) -> Option<bool> {
    let matches = match (action, command) {
        (
            ScenarioAction::DescribeShareGroup(action),
            AdapterCommand::DescribeShareGroup(command),
        ) => {
            action.client_id == command.client_id
                && action.operation_id == command.operation_id
                && action.group_id == command.group_id
                && action.include_authorized_operations == command.include_authorized_operations
                && action.timeout_ms == command.timeout_ms
        }
        (
            ScenarioAction::DescribeShareGroups(action),
            AdapterCommand::DescribeShareGroups(command),
        ) => {
            action.client_id == command.client_id
                && action.operation_id == command.operation_id
                && action
                    .groups
                    .iter()
                    .map(|group| group.group_id.as_str())
                    .eq(command.group_ids.iter().map(String::as_str))
                && action.include_authorized_operations == command.include_authorized_operations
                && action.timeout_ms == command.timeout_ms
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
        (
            ScenarioAction::ListShareGroupsOffsets(action),
            AdapterCommand::ListShareGroupsOffsets(command),
        ) => {
            action.client_id == command.client_id
                && action.operation_id == command.operation_id
                && action.groups.len() == command.groups.len()
                && action
                    .groups
                    .iter()
                    .zip(&command.groups)
                    .all(|(action, command)| {
                        action.group_id == command.group_id
                            && action.partitions.len() == command.partitions.len()
                            && action.partitions.iter().zip(&command.partitions).all(
                                |(action, command)| {
                                    action.topic == command.topic
                                        && action.partition == command.partition
                                },
                            )
                    })
                && action.timeout_ms == command.timeout_ms
        }
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
        (ScenarioAction::DeleteShareGroups(action), AdapterCommand::DeleteShareGroups(command)) => {
            action.client_id == command.client_id
                && action.operation_id == command.operation_id
                && action.group_ids == command.group_ids
                && action.timeout_ms == command.timeout_ms
        }
        (
            ScenarioAction::DescribeShareGroup(_)
            | ScenarioAction::DescribeShareGroups(_)
            | ScenarioAction::ListShareGroupOffsets(_)
            | ScenarioAction::ListShareGroupsOffsets(_)
            | ScenarioAction::AlterShareGroupOffsets(_)
            | ScenarioAction::DeleteShareGroupOffsets(_)
            | ScenarioAction::DeleteShareGroups(_),
            _,
        )
        | (
            _,
            AdapterCommand::DescribeShareGroup(_)
            | AdapterCommand::DescribeShareGroups(_)
            | AdapterCommand::ListShareGroupOffsets(_)
            | AdapterCommand::ListShareGroupsOffsets(_)
            | AdapterCommand::AlterShareGroupOffsets(_)
            | AdapterCommand::DeleteShareGroupOffsets(_)
            | AdapterCommand::DeleteShareGroups(_),
        ) => false,
        _ => return None,
    };
    Some(matches)
}
