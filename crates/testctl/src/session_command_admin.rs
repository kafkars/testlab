//! Admin scenario actions translate into exact public-operation completions.

use testlab_schema::{AdapterCommand, ScenarioAction};

use crate::runner_protocol::ExpectedEvent;

pub(crate) fn translate(action: &ScenarioAction) -> Option<(AdapterCommand, ExpectedEvent)> {
    let pair = match action {
        ScenarioAction::CreateTopic {
            client_id,
            operation_id,
            topic,
            partitions,
            replication_factor,
            timeout_ms,
        } => (
            AdapterCommand::CreateTopic {
                client_id: client_id.clone(),
                operation_id: operation_id.clone(),
                topic: topic.clone(),
                partitions: *partitions,
                replication_factor: *replication_factor,
                timeout_ms: *timeout_ms,
            },
            ExpectedEvent::TopicCreated {
                operation_id: operation_id.clone(),
                topic: topic.clone(),
            },
        ),
        ScenarioAction::CreatePartitions(action) => (
            AdapterCommand::CreatePartitions {
                client_id: action.client_id.clone(),
                operation_id: action.operation_id.clone(),
                topic: action.topic.clone(),
                total_count: action.total_count,
                timeout_ms: action.timeout_ms,
            },
            ExpectedEvent::TopicPartitionsCreated {
                operation_id: action.operation_id.clone(),
                topic: action.topic.clone(),
            },
        ),
        action @ (ScenarioAction::DescribeTopic(_)
        | ScenarioAction::ListTopics(_)
        | ScenarioAction::ListOffsets(_)) => return translate_query(action),
        _ => return None,
    };
    Some(pair)
}

fn translate_query(action: &ScenarioAction) -> Option<(AdapterCommand, ExpectedEvent)> {
    let pair = match action {
        ScenarioAction::DescribeTopic(action) => (
            AdapterCommand::DescribeTopic {
                client_id: action.client_id.clone(),
                operation_id: action.operation_id.clone(),
                topic: action.topic.clone(),
                timeout_ms: action.timeout_ms,
            },
            ExpectedEvent::TopicDescribed {
                operation_id: action.operation_id.clone(),
                topic: action.topic.clone(),
            },
        ),
        ScenarioAction::ListTopics(action) => (
            AdapterCommand::ListTopics {
                client_id: action.client_id.clone(),
                operation_id: action.operation_id.clone(),
                include_internal: action.include_internal,
                timeout_ms: action.timeout_ms,
            },
            ExpectedEvent::TopicsListed {
                operation_id: action.operation_id.clone(),
            },
        ),
        ScenarioAction::ListOffsets(action) => (
            AdapterCommand::ListOffsets {
                client_id: action.client_id.clone(),
                operation_id: action.operation_id.clone(),
                topic: action.topic.clone(),
                partition: action.partition,
                position: action.position,
                timeout_ms: action.timeout_ms,
            },
            ExpectedEvent::OffsetListed {
                operation_id: action.operation_id.clone(),
                topic: action.topic.clone(),
                partition: action.partition,
            },
        ),
        _ => return None,
    };
    Some(pair)
}
