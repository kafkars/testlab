//! Batch topic matching removes scenario-only expected outcomes from exact wire comparison.

use testlab_schema::{AdapterCommand, OperationId, ScenarioAction};

pub(super) fn action_operation_id(action: &ScenarioAction) -> Option<&OperationId> {
    match action {
        ScenarioAction::CreateTopicsBatch(value) => Some(&value.operation_id),
        ScenarioAction::DeleteTopics(value) => Some(&value.operation_id),
        ScenarioAction::DescribeTopics(value) => Some(&value.operation_id),
        ScenarioAction::DescribeTopicConfigs(value) => Some(&value.operation_id),
        ScenarioAction::ListOffsetsBatch(value) => Some(&value.operation_id),
        _ => None,
    }
}

pub(super) fn command_operation_id(command: &AdapterCommand) -> Option<&OperationId> {
    match command {
        AdapterCommand::CreateTopicsBatch(value) => Some(&value.operation_id),
        AdapterCommand::DeleteTopics(value) => Some(&value.operation_id),
        AdapterCommand::DescribeTopics(value) => Some(&value.operation_id),
        AdapterCommand::DescribeTopicConfigs(value) => Some(&value.operation_id),
        AdapterCommand::ListOffsetsBatch(value) => Some(&value.operation_id),
        _ => None,
    }
}

pub(super) fn matches(action: &ScenarioAction, command: &AdapterCommand) -> Option<bool> {
    match (action, command) {
        (ScenarioAction::CreateTopicsBatch(action), AdapterCommand::CreateTopicsBatch(command)) => {
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
        (ScenarioAction::ListOffsetsBatch(action), AdapterCommand::ListOffsetsBatch(command)) => {
            Some(
                action.client_id == command.client_id
                    && action.operation_id == command.operation_id
                    && action.timeout_ms == command.timeout_ms
                    && action.queries.len() == command.queries.len()
                    && action
                        .queries
                        .iter()
                        .zip(&command.queries)
                        .all(|(expected, actual)| {
                            expected.topic == actual.topic
                                && expected.partition == actual.partition
                                && expected.position == actual.position
                        }),
            )
        }
        (ScenarioAction::DescribeTopics(action), AdapterCommand::DescribeTopics(command)) => Some(
            action.client_id == command.client_id
                && action.operation_id == command.operation_id
                && action.timeout_ms == command.timeout_ms
                && action
                    .topics
                    .iter()
                    .map(|topic| topic.topic.as_str())
                    .eq(command.topics.iter().map(String::as_str)),
        ),
        (
            ScenarioAction::DescribeTopicConfigs(action),
            AdapterCommand::DescribeTopicConfigs(command),
        ) => Some(
            action.client_id == command.client_id
                && action.operation_id == command.operation_id
                && action.timeout_ms == command.timeout_ms
                && action
                    .topics
                    .iter()
                    .map(|selected| (selected.topic.as_str(), selected.config_name.as_str()))
                    .eq(command
                        .topics
                        .iter()
                        .map(|selected| (selected.topic.as_str(), selected.config_name.as_str()))),
        ),
        (ScenarioAction::DeleteTopics(action), AdapterCommand::DeleteTopics(command)) => Some(
            action.client_id == command.client_id
                && action.operation_id == command.operation_id
                && action.timeout_ms == command.timeout_ms
                && action
                    .topics
                    .iter()
                    .map(|topic| topic.topic.as_str())
                    .eq(command.topics.iter().map(String::as_str)),
        ),
        _ => None,
    }
}
