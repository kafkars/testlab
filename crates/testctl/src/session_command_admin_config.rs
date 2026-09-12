//! Topic-configuration scenario actions translate without leaking expected values.

use testlab_schema::{
    AdapterCommand, AlterTopicConfigCommand, AlterTopicConfigsCommand, DescribeTopicConfigCommand,
    DescribeTopicConfigsCommand, ListConfigResourcesCommand, ScenarioAction, TopicConfigAlteration,
    TopicConfigSelection,
};

use crate::runner_protocol::ExpectedEvent;

pub(super) fn translate(action: &ScenarioAction) -> Option<(AdapterCommand, ExpectedEvent)> {
    Some(match action {
        ScenarioAction::ListConfigResources(action) => (
            AdapterCommand::ListConfigResources(ListConfigResourcesCommand {
                client_id: action.client_id.clone(),
                operation_id: action.operation_id.clone(),
                api: action.api,
                timeout_ms: action.timeout_ms,
            }),
            ExpectedEvent::ConfigResourcesListed(action.operation_id.clone()),
        ),
        ScenarioAction::DescribeTopicConfig(action) => (
            AdapterCommand::DescribeTopicConfig(DescribeTopicConfigCommand {
                client_id: action.client_id.clone(),
                operation_id: action.operation_id.clone(),
                topic: action.topic.clone(),
                config_name: action.config_name.clone(),
                timeout_ms: action.timeout_ms,
            }),
            ExpectedEvent::TopicConfigDescribed {
                operation_id: action.operation_id.clone(),
                topic: action.topic.clone(),
                config_name: action.config_name.clone(),
            },
        ),
        ScenarioAction::DescribeTopicConfigs(action) => {
            let topics = action
                .topics
                .iter()
                .map(|selected| TopicConfigSelection {
                    topic: selected.topic.clone(),
                    config_name: selected.config_name.clone(),
                })
                .collect::<Vec<_>>();
            (
                AdapterCommand::DescribeTopicConfigs(DescribeTopicConfigsCommand {
                    client_id: action.client_id.clone(),
                    operation_id: action.operation_id.clone(),
                    api: action.api,
                    include_synonyms: action.include_synonyms,
                    include_documentation: action.include_documentation,
                    topics: topics.clone(),
                    timeout_ms: action.timeout_ms,
                }),
                ExpectedEvent::TopicConfigsDescribed {
                    operation_id: action.operation_id.clone(),
                    topics: topics
                        .into_iter()
                        .map(|selected| (selected.topic, selected.config_name))
                        .collect(),
                },
            )
        }
        ScenarioAction::AlterTopicConfigs(action) => {
            let topics = action
                .topics
                .iter()
                .map(|selected| TopicConfigAlteration {
                    topic: selected.topic.clone(),
                    config_name: selected.config_name.clone(),
                    method: selected.method,
                    value: selected.command_value().map(str::to_owned),
                })
                .collect::<Vec<_>>();
            (
                AdapterCommand::AlterTopicConfigs(AlterTopicConfigsCommand {
                    client_id: action.client_id.clone(),
                    operation_id: action.operation_id.clone(),
                    api: action.api,
                    topics: topics.clone(),
                    timeout_ms: action.timeout_ms,
                }),
                ExpectedEvent::TopicConfigsAltered {
                    operation_id: action.operation_id.clone(),
                    topics: topics
                        .into_iter()
                        .map(|selected| (selected.topic, selected.config_name))
                        .collect(),
                },
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
            config_alteration_event(action),
        ),
        _ => return None,
    })
}

fn config_alteration_event(action: &testlab_schema::AlterTopicConfigAction) -> ExpectedEvent {
    if action.validate_only {
        ExpectedEvent::TopicConfigAlterationValidated {
            operation_id: action.operation_id.clone(),
            topic: action.topic.clone(),
            config_name: action.config_name.clone(),
        }
    } else {
        ExpectedEvent::TopicConfigAltered {
            operation_id: action.operation_id.clone(),
            topic: action.topic.clone(),
            config_name: action.config_name.clone(),
        }
    }
}
