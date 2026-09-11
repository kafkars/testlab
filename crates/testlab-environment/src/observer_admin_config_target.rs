//! Topic-configuration targets require exact scenario and wire identities.

use testlab_schema::{
    AdapterCommand, AlterTopicConfigCommand, AlterTopicConfigsCommand, DescribeTopicConfigCommand,
    DescribeTopicConfigsCommand, ScenarioAction, TopicConfigAlteration, TopicConfigSelection,
};

use crate::observer_admin_target::{
    AdminTarget, ConfigBatchTarget, ConfigTarget, TargetMatch, unique,
};
use crate::observer_error::ObserverError;

pub(super) fn match_action(action: &ScenarioAction) -> Result<Option<TargetMatch>, ObserverError> {
    Ok(Some(match action {
        ScenarioAction::DescribeTopicConfig(action) => (
            AdapterCommand::DescribeTopicConfig(DescribeTopicConfigCommand {
                client_id: action.client_id.clone(),
                operation_id: action.operation_id.clone(),
                topic: action.topic.clone(),
                config_name: action.config_name.clone(),
                timeout_ms: action.timeout_ms,
            }),
            AdminTarget::TopicConfig(ConfigTarget {
                operation_id: action.operation_id.clone(),
                topic: action.topic.clone(),
                config_name: action.config_name.clone(),
                expected_value: action.expected_value.clone(),
                poll_expected: false,
            }),
        ),
        ScenarioAction::DescribeTopicConfigs(action) => {
            let topics = action
                .topics
                .iter()
                .map(|selected| selected.topic.clone())
                .collect::<Vec<_>>();
            unique(&topics, &action.operation_id, "topics")?;
            (
                AdapterCommand::DescribeTopicConfigs(DescribeTopicConfigsCommand {
                    client_id: action.client_id.clone(),
                    operation_id: action.operation_id.clone(),
                    topics: action
                        .topics
                        .iter()
                        .map(|selected| TopicConfigSelection {
                            topic: selected.topic.clone(),
                            config_name: selected.config_name.clone(),
                        })
                        .collect(),
                    timeout_ms: action.timeout_ms,
                }),
                AdminTarget::TopicConfigs(ConfigBatchTarget {
                    operation_id: action.operation_id.clone(),
                    configs: action
                        .topics
                        .iter()
                        .map(|selected| ConfigTarget {
                            operation_id: action.operation_id.clone(),
                            topic: selected.topic.clone(),
                            config_name: selected.config_name.clone(),
                            expected_value: selected.expected_value.clone(),
                            poll_expected: false,
                        })
                        .collect(),
                }),
            )
        }
        ScenarioAction::AlterTopicConfigs(action) => {
            let topics = action
                .topics
                .iter()
                .map(|selected| selected.topic.clone())
                .collect::<Vec<_>>();
            unique(&topics, &action.operation_id, "topics")?;
            (
                AdapterCommand::AlterTopicConfigs(AlterTopicConfigsCommand {
                    client_id: action.client_id.clone(),
                    operation_id: action.operation_id.clone(),
                    topics: action
                        .topics
                        .iter()
                        .map(|selected| TopicConfigAlteration {
                            topic: selected.topic.clone(),
                            config_name: selected.config_name.clone(),
                            value: selected.value.clone(),
                        })
                        .collect(),
                    timeout_ms: action.timeout_ms,
                }),
                AdminTarget::TopicConfigs(ConfigBatchTarget {
                    operation_id: action.operation_id.clone(),
                    configs: action
                        .topics
                        .iter()
                        .map(|selected| ConfigTarget {
                            operation_id: action.operation_id.clone(),
                            topic: selected.topic.clone(),
                            config_name: selected.config_name.clone(),
                            expected_value: selected.value.clone(),
                            poll_expected: true,
                        })
                        .collect(),
                }),
            )
        }
        ScenarioAction::AlterTopicConfig(action) => (
            AdapterCommand::AlterTopicConfig(AlterTopicConfigCommand {
                client_id: action.client_id.clone(),
                operation_id: action.operation_id.clone(),
                topic: action.topic.clone(),
                config_name: action.config_name.clone(),
                value: action.value.clone(),
                validate_only: action.validate_only,
                timeout_ms: action.timeout_ms,
            }),
            AdminTarget::TopicConfig(ConfigTarget {
                operation_id: action.operation_id.clone(),
                topic: action.topic.clone(),
                config_name: action.config_name.clone(),
                expected_value: if action.validate_only {
                    action.expected_current_value.clone().ok_or_else(|| {
                        ObserverError::InvalidTarget(format!(
                            "validate-only admin operation {} omitted expected current configuration value",
                            action.operation_id
                        ))
                    })?
                } else {
                    action.value.clone()
                },
                poll_expected: !action.validate_only,
            }),
        ),
        _ => return Ok(None),
    }))
}
