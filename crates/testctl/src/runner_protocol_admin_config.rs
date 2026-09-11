//! Topic-configuration protocol matching enforces every stable identity.

use testlab_schema::AdapterEvent;

use crate::run_error::RunFailure;
use crate::runner_protocol::{EventDisposition, ExpectedEvent};

pub(super) fn classify(
    expected: &ExpectedEvent,
    event: &AdapterEvent,
) -> Option<Result<EventDisposition, RunFailure>> {
    let matches = match (expected, event) {
        (
            ExpectedEvent::ConfigResourcesListed(operation_id),
            AdapterEvent::ConfigResourcesListed(actual),
        ) => operation_id == &actual.operation_id,
        (
            ExpectedEvent::TopicConfigDescribed {
                operation_id,
                topic,
                config_name,
            },
            AdapterEvent::TopicConfigDescribed(actual),
        ) => {
            operation_id == &actual.operation_id
                && topic == &actual.topic
                && config_name == &actual.config_name
        }
        (
            ExpectedEvent::TopicConfigsDescribed {
                operation_id,
                topics,
            },
            AdapterEvent::TopicConfigsDescribed(actual),
        ) => {
            operation_id == &actual.operation_id
                && actual
                    .outcomes
                    .iter()
                    .map(|outcome| (outcome.topic.as_str(), outcome.config_name.as_str()))
                    .eq(topics
                        .iter()
                        .map(|(topic, config_name)| (topic.as_str(), config_name.as_str())))
        }
        (
            ExpectedEvent::TopicConfigsAltered {
                operation_id,
                topics,
            },
            AdapterEvent::TopicConfigsAltered(actual),
        ) => {
            operation_id == &actual.operation_id
                && actual
                    .outcomes
                    .iter()
                    .map(|outcome| (outcome.topic.as_str(), outcome.config_name.as_str()))
                    .eq(topics
                        .iter()
                        .map(|(topic, config_name)| (topic.as_str(), config_name.as_str())))
        }
        (
            ExpectedEvent::TopicConfigAltered {
                operation_id,
                topic,
                config_name,
            },
            AdapterEvent::TopicConfigAltered(actual),
        )
        | (
            ExpectedEvent::TopicConfigAlterationValidated {
                operation_id,
                topic,
                config_name,
            },
            AdapterEvent::TopicConfigAlterationValidated(actual),
        ) => {
            operation_id == &actual.operation_id
                && topic == &actual.topic
                && config_name == &actual.config_name
        }
        _ => return None,
    };
    Some(if matches {
        Ok(EventDisposition::Complete)
    } else {
        Err(RunFailure::protocol(
            "event_identity_mismatch",
            format!("event {event:?} does not match expected {expected:?}"),
        ))
    })
}

pub(super) fn expected(expected: &ExpectedEvent) -> bool {
    matches!(
        expected,
        ExpectedEvent::ConfigResourcesListed(_)
            | ExpectedEvent::TopicConfigDescribed { .. }
            | ExpectedEvent::TopicConfigsDescribed { .. }
            | ExpectedEvent::TopicConfigsAltered { .. }
            | ExpectedEvent::TopicConfigAltered { .. }
            | ExpectedEvent::TopicConfigAlterationValidated { .. }
    )
}

pub(super) fn event(event: &AdapterEvent) -> bool {
    matches!(
        event,
        AdapterEvent::ConfigResourcesListed(_)
            | AdapterEvent::TopicConfigDescribed(_)
            | AdapterEvent::TopicConfigsDescribed(_)
            | AdapterEvent::TopicConfigsAltered(_)
            | AdapterEvent::TopicConfigAltered(_)
            | AdapterEvent::TopicConfigAlterationValidated(_)
    )
}
