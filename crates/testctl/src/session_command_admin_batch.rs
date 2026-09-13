//! Batch admin translation keeps expected per-resource results inside the harness.

use testlab_schema::{
    AdapterCommand, CreateTopicBatchCommandItem, CreateTopicsBatchCommand, DeleteTopicsCommand,
    DescribeTopicsCommand, ListOffsetsBatchCommand, OffsetListingSelection, ScenarioAction,
};

use crate::runner_protocol::ExpectedEvent;

pub(crate) fn translate(action: &ScenarioAction) -> Option<(AdapterCommand, ExpectedEvent)> {
    match action {
        ScenarioAction::CreateTopicsBatch(action) => Some(create_topics(action)),
        ScenarioAction::DeleteTopics(action) => Some(delete_topics(action)),
        ScenarioAction::DescribeTopics(action) => Some(describe_topics(action)),
        ScenarioAction::ListOffsetsBatch(action) => Some(list_offsets(action)),
        _ => None,
    }
}

fn delete_topics(action: &testlab_schema::DeleteTopicsAction) -> (AdapterCommand, ExpectedEvent) {
    let topics = action
        .topics
        .iter()
        .map(|topic| topic.topic.clone())
        .collect::<Vec<_>>();
    (
        AdapterCommand::DeleteTopics(DeleteTopicsCommand {
            client_id: action.client_id.clone(),
            operation_id: action.operation_id.clone(),
            selection: action.selection,
            topics: topics.clone(),
            timeout_ms: action.timeout_ms,
        }),
        ExpectedEvent::TopicsDeleted {
            operation_id: action.operation_id.clone(),
            topics,
        },
    )
}

fn describe_topics(
    action: &testlab_schema::DescribeTopicsAction,
) -> (AdapterCommand, ExpectedEvent) {
    let topics = action
        .topics
        .iter()
        .map(|topic| topic.topic.clone())
        .collect::<Vec<_>>();
    (
        AdapterCommand::DescribeTopics(DescribeTopicsCommand {
            client_id: action.client_id.clone(),
            operation_id: action.operation_id.clone(),
            selection: action.selection,
            topics: topics.clone(),
            include_authorized_operations: action.include_authorized_operations,
            timeout_ms: action.timeout_ms,
        }),
        ExpectedEvent::TopicsDescribed {
            operation_id: action.operation_id.clone(),
            topics,
        },
    )
}

fn create_topics(
    action: &testlab_schema::CreateTopicsBatchAction,
) -> (AdapterCommand, ExpectedEvent) {
    let topics = action
        .topics
        .iter()
        .map(|item| CreateTopicBatchCommandItem {
            topic: item.topic.clone(),
            partitions: item.partitions,
            replication_factor: item.replication_factor,
            configs: item.configs.clone(),
        })
        .collect();
    (
        AdapterCommand::CreateTopicsBatch(CreateTopicsBatchCommand {
            client_id: action.client_id.clone(),
            operation_id: action.operation_id.clone(),
            topics,
            timeout_ms: action.timeout_ms,
        }),
        ExpectedEvent::TopicsCreationCompleted {
            operation_id: action.operation_id.clone(),
        },
    )
}

fn list_offsets(
    action: &testlab_schema::ListOffsetsBatchAction,
) -> (AdapterCommand, ExpectedEvent) {
    let queries = action
        .queries
        .iter()
        .map(|query| OffsetListingSelection {
            topic: query.topic.clone(),
            partition: query.partition,
            position: query.position,
        })
        .collect();
    (
        AdapterCommand::ListOffsetsBatch(ListOffsetsBatchCommand {
            client_id: action.client_id.clone(),
            operation_id: action.operation_id.clone(),
            read_isolation: action.read_isolation,
            queries,
            timeout_ms: action.timeout_ms,
        }),
        ExpectedEvent::OffsetsListed {
            operation_id: action.operation_id.clone(),
        },
    )
}
