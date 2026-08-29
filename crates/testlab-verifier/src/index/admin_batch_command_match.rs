//! Batch topic matching removes scenario-only expected outcomes from exact wire comparison.

use testlab_schema::{AdapterCommand, OperationId, ScenarioAction};

pub(super) fn action_operation_id(action: &ScenarioAction) -> Option<&OperationId> {
    match action {
        ScenarioAction::CreateTopicsBatch(value) => Some(&value.operation_id),
        _ => None,
    }
}

pub(super) fn command_operation_id(command: &AdapterCommand) -> Option<&OperationId> {
    match command {
        AdapterCommand::CreateTopicsBatch(value) => Some(&value.operation_id),
        _ => None,
    }
}

pub(super) fn matches(action: &ScenarioAction, command: &AdapterCommand) -> Option<bool> {
    let (ScenarioAction::CreateTopicsBatch(action), AdapterCommand::CreateTopicsBatch(command)) =
        (action, command)
    else {
        return None;
    };
    Some(
        action.client_id == command.client_id
            && action.operation_id == command.operation_id
            && action.timeout_ms == command.timeout_ms
            && action.topics.len() == command.topics.len()
            && action
                .topics
                .iter()
                .zip(&command.topics)
                .all(|(expected, actual)| {
                    expected.topic == actual.topic
                        && expected.partitions == actual.partitions
                        && expected.replication_factor == actual.replication_factor
                }),
    )
}
