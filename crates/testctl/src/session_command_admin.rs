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
        ScenarioAction::CreatePartitions {
            client_id,
            operation_id,
            topic,
            total_count,
            timeout_ms,
        } => (
            AdapterCommand::CreatePartitions {
                client_id: client_id.clone(),
                operation_id: operation_id.clone(),
                topic: topic.clone(),
                total_count: *total_count,
                timeout_ms: *timeout_ms,
            },
            ExpectedEvent::TopicPartitionsCreated {
                operation_id: operation_id.clone(),
                topic: topic.clone(),
            },
        ),
        action @ (ScenarioAction::DescribeTopic { .. }
        | ScenarioAction::ListTopics { .. }
        | ScenarioAction::ListOffsets { .. }) => return translate_query(action),
        _ => return None,
    };
    Some(pair)
}

fn translate_query(action: &ScenarioAction) -> Option<(AdapterCommand, ExpectedEvent)> {
    let pair = match action {
        ScenarioAction::DescribeTopic {
            client_id,
            operation_id,
            topic,
            timeout_ms,
            ..
        } => (
            AdapterCommand::DescribeTopic {
                client_id: client_id.clone(),
                operation_id: operation_id.clone(),
                topic: topic.clone(),
                timeout_ms: *timeout_ms,
            },
            ExpectedEvent::TopicDescribed {
                operation_id: operation_id.clone(),
                topic: topic.clone(),
            },
        ),
        ScenarioAction::ListTopics {
            client_id,
            operation_id,
            include_internal,
            timeout_ms,
            ..
        } => (
            AdapterCommand::ListTopics {
                client_id: client_id.clone(),
                operation_id: operation_id.clone(),
                include_internal: *include_internal,
                timeout_ms: *timeout_ms,
            },
            ExpectedEvent::TopicsListed {
                operation_id: operation_id.clone(),
            },
        ),
        ScenarioAction::ListOffsets {
            client_id,
            operation_id,
            topic,
            partition,
            position,
            timeout_ms,
            ..
        } => (
            AdapterCommand::ListOffsets {
                client_id: client_id.clone(),
                operation_id: operation_id.clone(),
                topic: topic.clone(),
                partition: *partition,
                position: *position,
                timeout_ms: *timeout_ms,
            },
            ExpectedEvent::OffsetListed {
                operation_id: operation_id.clone(),
                topic: topic.clone(),
                partition: *partition,
            },
        ),
        _ => return None,
    };
    Some(pair)
}
