//! User SCRAM command matching binds exact intent to one secret-free wire request.

use testlab_schema::{AdapterCommand, OperationId, ScenarioAction};

pub(super) fn action_operation_id(action: &ScenarioAction) -> Option<&OperationId> {
    match action {
        ScenarioAction::AlterUserScramCredential(value) => Some(&value.operation_id),
        ScenarioAction::DescribeUserScramCredential(value) => Some(&value.operation_id),
        _ => None,
    }
}

pub(super) fn command_operation_id(command: &AdapterCommand) -> Option<&OperationId> {
    match command {
        AdapterCommand::AlterUserScramCredential(value) => Some(&value.operation_id),
        AdapterCommand::DescribeUserScramCredential(value) => Some(&value.operation_id),
        _ => None,
    }
}

pub(super) fn matches(action: &ScenarioAction, command: &AdapterCommand) -> Option<bool> {
    let matches = match (action, command) {
        (
            ScenarioAction::AlterUserScramCredential(action),
            AdapterCommand::AlterUserScramCredential(command),
        ) => action == command,
        (
            ScenarioAction::DescribeUserScramCredential(action),
            AdapterCommand::DescribeUserScramCredential(command),
        ) => {
            action.client_id == command.client_id
                && action.operation_id == command.operation_id
                && action.user == command.user
                && action.mechanism == command.mechanism
                && action.timeout_ms == command.timeout_ms
        }
        (
            ScenarioAction::AlterUserScramCredential(_)
            | ScenarioAction::DescribeUserScramCredential(_),
            _,
        )
        | (
            _,
            AdapterCommand::AlterUserScramCredential(_)
            | AdapterCommand::DescribeUserScramCredential(_),
        ) => false,
        _ => return None,
    };
    Some(matches)
}
