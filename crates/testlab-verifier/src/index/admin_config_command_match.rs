//! Exact topic-configuration matching keeps scenario expectations off the wire.

use testlab_schema::{AdapterCommand, OperationId, ScenarioAction};

use super::HistoryIndex;

impl HistoryIndex {
    pub(crate) fn topic_config_altered_between(
        &self,
        topic: &str,
        config_name: &str,
        after: u64,
        before: u64,
    ) -> bool {
        self.admin_commands.iter().any(|(sequence, _, command)| {
            after < *sequence
                && *sequence < before
                && matches!(
                    command,
                    AdapterCommand::AlterTopicConfig(value)
                        if value.topic == topic && value.config_name == config_name
                )
        })
    }
}

pub(super) fn action_operation_id(action: &ScenarioAction) -> Option<&OperationId> {
    Some(match action {
        ScenarioAction::DescribeTopicConfig(value) => &value.operation_id,
        ScenarioAction::AlterTopicConfig(value) => &value.operation_id,
        _ => return None,
    })
}

pub(super) fn command_operation_id(command: &AdapterCommand) -> Option<&OperationId> {
    Some(match command {
        AdapterCommand::DescribeTopicConfig(value) => &value.operation_id,
        AdapterCommand::AlterTopicConfig(value) => &value.operation_id,
        _ => return None,
    })
}

pub(super) fn matches(action: &ScenarioAction, command: &AdapterCommand) -> Option<bool> {
    Some(match (action, command) {
        (
            ScenarioAction::DescribeTopicConfig(action),
            AdapterCommand::DescribeTopicConfig(command),
        ) => same_topic(action, command) && action.config_name == command.config_name,
        (ScenarioAction::AlterTopicConfig(action), AdapterCommand::AlterTopicConfig(command)) => {
            action.client_id == command.client_id
                && action.operation_id == command.operation_id
                && action.topic == command.topic
                && action.config_name == command.config_name
                && action.value == command.value
                && action.validate_only == command.validate_only
                && action.timeout_ms == command.timeout_ms
        }
        _ => return None,
    })
}

fn same_topic(
    action: &testlab_schema::DescribeTopicConfigAction,
    command: &testlab_schema::DescribeTopicConfigCommand,
) -> bool {
    action.client_id == command.client_id
        && action.operation_id == command.operation_id
        && action.topic == command.topic
        && action.timeout_ms == command.timeout_ms
}
