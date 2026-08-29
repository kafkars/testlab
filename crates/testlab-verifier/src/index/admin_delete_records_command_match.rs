//! `DeleteRecords` command matching keeps scenario-only watermark expectations off the wire.

use testlab_schema::{AdapterCommand, OperationId, ScenarioAction};

pub(super) fn action_operation_id(action: &ScenarioAction) -> Option<&OperationId> {
    match action {
        ScenarioAction::DeleteRecords(value) => Some(&value.operation_id),
        _ => None,
    }
}

pub(super) fn command_operation_id(command: &AdapterCommand) -> Option<&OperationId> {
    match command {
        AdapterCommand::DeleteRecords(value) => Some(&value.operation_id),
        _ => None,
    }
}

pub(super) fn matches(action: &ScenarioAction, command: &AdapterCommand) -> Option<bool> {
    let (ScenarioAction::DeleteRecords(action), AdapterCommand::DeleteRecords(command)) =
        (action, command)
    else {
        return None;
    };
    Some(
        action.client_id == command.client_id
            && action.operation_id == command.operation_id
            && action.topic == command.topic
            && action.partition == command.partition
            && action.before_offset == command.before_offset
            && action.timeout_ms == command.timeout_ms,
    )
}
