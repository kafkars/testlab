//! `DeleteRecords` command matching keeps scenario-only watermark expectations off the wire.

use testlab_schema::{AdapterCommand, OperationId, ScenarioAction};

pub(super) fn action_operation_id(action: &ScenarioAction) -> Option<&OperationId> {
    match action {
        ScenarioAction::DeleteRecords(value) => Some(&value.operation_id),
        ScenarioAction::DeleteRecordsBatch(value) => Some(&value.operation_id),
        _ => None,
    }
}

pub(super) fn command_operation_id(command: &AdapterCommand) -> Option<&OperationId> {
    match command {
        AdapterCommand::DeleteRecords(value) => Some(&value.operation_id),
        AdapterCommand::DeleteRecordsBatch(value) => Some(&value.operation_id),
        _ => None,
    }
}

pub(super) fn matches(action: &ScenarioAction, command: &AdapterCommand) -> Option<bool> {
    Some(match (action, command) {
        (ScenarioAction::DeleteRecords(action), AdapterCommand::DeleteRecords(command)) => {
            action.client_id == command.client_id
                && action.operation_id == command.operation_id
                && action.topic == command.topic
                && action.partition == command.partition
                && action.before_offset == command.before_offset
                && action.timeout_ms == command.timeout_ms
        }
        (
            ScenarioAction::DeleteRecordsBatch(action),
            AdapterCommand::DeleteRecordsBatch(command),
        ) => {
            action.client_id == command.client_id
                && action.operation_id == command.operation_id
                && action.timeout_ms == command.timeout_ms
                && action.targets.len() == command.targets.len()
                && action
                    .targets
                    .iter()
                    .zip(&command.targets)
                    .all(|(action, command)| {
                        action.topic == command.topic
                            && action.partition == command.partition
                            && action.boundary == command.boundary
                    })
        }
        (ScenarioAction::DeleteRecords(_) | ScenarioAction::DeleteRecordsBatch(_), _)
        | (_, AdapterCommand::DeleteRecords(_) | AdapterCommand::DeleteRecordsBatch(_)) => false,
        _ => return None,
    })
}
