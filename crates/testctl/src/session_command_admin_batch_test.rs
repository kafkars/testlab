//! Batch admin translation preserves ordering while omitting scenario-only outcomes.

use testlab_schema::{
    AdapterCommand, AdminOffsetPosition, AdminReadIsolation, ClientId, CreateTopicBatchActionItem,
    CreateTopicBatchCommandItem, CreateTopicsBatchAction, CreateTopicsBatchCommand,
    ListOffsetsBatchAction, ListOffsetsBatchCommand, OffsetListingExpectation,
    OffsetListingSelection, OperationId, ScenarioAction, TOPIC_ALREADY_EXISTS_ERROR_CODE,
    TopicCreationConfig,
};

use crate::runner_protocol::ExpectedEvent;

#[test]
fn batch_translation_is_one_ordered_command_without_expectations() {
    let client_id = ClientId::new("client-1").unwrap_or_else(|error| panic!("client: {error}"));
    let operation_id =
        OperationId::new("batch-create").unwrap_or_else(|error| panic!("operation: {error}"));
    let mut fresh = item("fresh", 2, None);
    fresh.configs = configs();
    let action = ScenarioAction::CreateTopicsBatch(CreateTopicsBatchAction {
        client_id: client_id.clone(),
        operation_id: operation_id.clone(),
        topics: vec![
            fresh,
            item(
                "existing",
                3,
                Some(TOPIC_ALREADY_EXISTS_ERROR_CODE.to_owned()),
            ),
        ],
        timeout_ms: 500,
    });

    let (command, expected) = super::session_command_admin::translate(&action)
        .unwrap_or_else(|| panic!("missing batch translation"));

    let mut fresh = command_item("fresh", 2);
    fresh.configs = configs();
    assert_eq!(
        command,
        AdapterCommand::CreateTopicsBatch(CreateTopicsBatchCommand {
            client_id,
            operation_id: operation_id.clone(),
            topics: vec![fresh, command_item("existing", 3)],
            timeout_ms: 500,
        })
    );
    assert!(matches!(
        expected,
        ExpectedEvent::TopicsCreationCompleted { operation_id: actual }
            if actual == operation_id
    ));
}

#[test]
fn offset_batch_translation_preserves_queries_without_expected_offsets() {
    let client_id = ClientId::new("client-1").unwrap_or_else(|error| panic!("client: {error}"));
    let operation_id =
        OperationId::new("batch-offsets").unwrap_or_else(|error| panic!("operation: {error}"));
    let action = ScenarioAction::ListOffsetsBatch(ListOffsetsBatchAction {
        client_id: client_id.clone(),
        operation_id: operation_id.clone(),
        read_isolation: AdminReadIsolation::ReadUncommitted,
        queries: vec![
            offset_expectation("records", 2, AdminOffsetPosition::Latest, 7),
            offset_expectation("records", 0, AdminOffsetPosition::Earliest, 0),
        ],
        timeout_ms: 500,
    });

    let (command, expected) = super::session_command_admin::translate(&action)
        .unwrap_or_else(|| panic!("missing offset batch translation"));

    assert_eq!(
        command,
        AdapterCommand::ListOffsetsBatch(ListOffsetsBatchCommand {
            client_id,
            operation_id: operation_id.clone(),
            read_isolation: AdminReadIsolation::ReadUncommitted,
            queries: vec![
                offset_selection("records", 2, AdminOffsetPosition::Latest),
                offset_selection("records", 0, AdminOffsetPosition::Earliest),
            ],
            timeout_ms: 500,
        })
    );
    assert!(matches!(
        expected,
        ExpectedEvent::OffsetsListed { operation_id: actual } if actual == operation_id
    ));
}

fn item(
    topic: &str,
    partitions: i32,
    expected_error_code: Option<String>,
) -> CreateTopicBatchActionItem {
    CreateTopicBatchActionItem {
        topic: topic.to_owned(),
        partitions,
        replication_factor: 1,
        configs: Vec::new(),
        expected_error_code,
    }
}

fn command_item(topic: &str, partitions: i32) -> CreateTopicBatchCommandItem {
    CreateTopicBatchCommandItem {
        topic: topic.to_owned(),
        partitions,
        replication_factor: 1,
        configs: Vec::new(),
    }
}

fn configs() -> Vec<TopicCreationConfig> {
    vec![TopicCreationConfig {
        name: "cleanup.policy".to_owned(),
        value: "compact".to_owned(),
    }]
}

fn offset_expectation(
    topic: &str,
    partition: i32,
    position: AdminOffsetPosition,
    expected_offset: i64,
) -> OffsetListingExpectation {
    OffsetListingExpectation {
        topic: topic.to_owned(),
        partition,
        position,
        expected_offset,
    }
}

fn offset_selection(
    topic: &str,
    partition: i32,
    position: AdminOffsetPosition,
) -> OffsetListingSelection {
    OffsetListingSelection {
        topic: topic.to_owned(),
        partition,
        position,
    }
}
