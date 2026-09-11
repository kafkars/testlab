//! Exact topic-configuration indexing and matching keeps expectations off the wire.

use testlab_schema::{AdapterCommand, AdapterEvent, OperationId, ScenarioAction};

use super::{
    HistoryIndex, IndexedAdminTopicConfigCompletion, IndexedAdminTopicConfigsDescription,
    IndexedTopicConfigDescription, admin_types::IndexedConfigBatch,
};

pub(super) fn record(index: &mut HistoryIndex, event: &AdapterEvent, sequence: u64) -> bool {
    match event {
        AdapterEvent::TopicConfigDescribed(value) => index
            .topic_configs_described
            .entry(value.operation_id.clone())
            .or_default()
            .push(IndexedTopicConfigDescription {
                history_sequence: sequence,
                topic: value.topic.clone(),
                config_name: value.config_name.clone(),
                value: value.value.clone(),
            }),
        AdapterEvent::TopicConfigsDescribed(value) => index
            .topic_configs_batch_described
            .entry(value.operation_id.clone())
            .or_default()
            .push(IndexedAdminTopicConfigsDescription {
                history_sequence: sequence,
                value: value.clone(),
            }),
        AdapterEvent::TopicConfigsAltered(value) => index
            .topic_configs_batch_altered
            .entry(value.operation_id.clone())
            .or_default()
            .push(IndexedConfigBatch {
                history_sequence: sequence,
                value: value.clone(),
            }),
        AdapterEvent::TopicConfigAltered(value) => index
            .topic_configs_altered
            .entry(value.operation_id.clone())
            .or_default()
            .push(IndexedAdminTopicConfigCompletion {
                history_sequence: sequence,
                topic: value.topic.clone(),
                config_name: value.config_name.clone(),
            }),
        _ => return false,
    }
    true
}

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
                && alters_topic_config(command, topic, config_name)
        })
    }
}

fn alters_topic_config(command: &AdapterCommand, topic: &str, config_name: &str) -> bool {
    match command {
        AdapterCommand::AlterTopicConfig(value) => {
            value.topic == topic && value.config_name == config_name
        }
        AdapterCommand::AlterTopicConfigs(value) => value
            .topics
            .iter()
            .any(|selected| selected.topic == topic && selected.config_name == config_name),
        _ => false,
    }
}

pub(super) fn action_operation_id(action: &ScenarioAction) -> Option<&OperationId> {
    Some(match action {
        ScenarioAction::DescribeTopicConfig(value) => &value.operation_id,
        ScenarioAction::AlterTopicConfigs(value) => &value.operation_id,
        ScenarioAction::AlterTopicConfig(value) => &value.operation_id,
        _ => return None,
    })
}

pub(super) fn command_operation_id(command: &AdapterCommand) -> Option<&OperationId> {
    Some(match command {
        AdapterCommand::DescribeTopicConfig(value) => &value.operation_id,
        AdapterCommand::AlterTopicConfigs(value) => &value.operation_id,
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
        (ScenarioAction::AlterTopicConfigs(action), AdapterCommand::AlterTopicConfigs(command)) => {
            action.client_id == command.client_id
                && action.operation_id == command.operation_id
                && action.timeout_ms == command.timeout_ms
                && action
                    .topics
                    .iter()
                    .map(|selected| {
                        (
                            selected.topic.as_str(),
                            selected.config_name.as_str(),
                            selected.value.as_str(),
                        )
                    })
                    .eq(command.topics.iter().map(|selected| {
                        (
                            selected.topic.as_str(),
                            selected.config_name.as_str(),
                            selected.value.as_str(),
                        )
                    }))
        }
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
