//! Batch admin translation keeps expected per-resource results inside the harness.

use testlab_schema::{
    AdapterCommand, CreateTopicBatchCommandItem, CreateTopicsBatchCommand, ScenarioAction,
};

use crate::runner_protocol::ExpectedEvent;

pub(crate) fn translate(action: &ScenarioAction) -> Option<(AdapterCommand, ExpectedEvent)> {
    let ScenarioAction::CreateTopicsBatch(action) = action else {
        return None;
    };
    let topics = action
        .topics
        .iter()
        .map(|item| CreateTopicBatchCommandItem {
            topic: item.topic.clone(),
            partitions: item.partitions,
            replication_factor: item.replication_factor,
        })
        .collect();
    Some((
        AdapterCommand::CreateTopicsBatch(CreateTopicsBatchCommand {
            client_id: action.client_id.clone(),
            operation_id: action.operation_id.clone(),
            topics,
            timeout_ms: action.timeout_ms,
        }),
        ExpectedEvent::TopicsCreationCompleted {
            operation_id: action.operation_id.clone(),
        },
    ))
}
