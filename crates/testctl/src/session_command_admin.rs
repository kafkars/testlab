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
        _ => return None,
    };
    Some(pair)
}
