//! ACL command matching keeps exact scenario intent tied to one wire operation.

use testlab_schema::{AdapterCommand, OperationId, ScenarioAction};

pub(super) fn action_operation_id(action: &ScenarioAction) -> Option<&OperationId> {
    match action {
        ScenarioAction::CreateAcls(value) => Some(&value.operation_id),
        ScenarioAction::DescribeAcls(value) => Some(&value.operation_id),
        ScenarioAction::DeleteAcls(value) => Some(&value.operation_id),
        _ => None,
    }
}

pub(super) fn command_operation_id(command: &AdapterCommand) -> Option<&OperationId> {
    match command {
        AdapterCommand::CreateAcls(value) => Some(&value.operation_id),
        AdapterCommand::DescribeAcls(value) => Some(&value.operation_id),
        AdapterCommand::DeleteAcls(value) => Some(&value.operation_id),
        _ => None,
    }
}

pub(super) fn matches(action: &ScenarioAction, command: &AdapterCommand) -> Option<bool> {
    let matches = match (action, command) {
        (ScenarioAction::CreateAcls(action), AdapterCommand::CreateAcls(command)) => {
            action == command
        }
        (ScenarioAction::DescribeAcls(action), AdapterCommand::DescribeAcls(command)) => {
            action == command
        }
        (ScenarioAction::DeleteAcls(action), AdapterCommand::DeleteAcls(command)) => {
            action == command
        }
        (
            ScenarioAction::CreateAcls(_)
            | ScenarioAction::DescribeAcls(_)
            | ScenarioAction::DeleteAcls(_),
            _,
        )
        | (
            _,
            AdapterCommand::CreateAcls(_)
            | AdapterCommand::DescribeAcls(_)
            | AdapterCommand::DeleteAcls(_),
        ) => false,
        _ => return None,
    };
    Some(matches)
}
