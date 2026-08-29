//! Topic-configuration scenario actions translate without leaking expected values.

use testlab_schema::{
    AdapterCommand, AlterTopicConfigCommand, DescribeTopicConfigCommand, ScenarioAction,
};

use crate::runner_protocol::ExpectedEvent;

pub(super) fn translate(action: &ScenarioAction) -> Option<(AdapterCommand, ExpectedEvent)> {
    Some(match action {
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
