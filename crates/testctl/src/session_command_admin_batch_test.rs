//! Batch admin translation preserves ordering while omitting scenario-only outcomes.

use testlab_schema::{
    AdapterCommand, ClientId, CreateTopicBatchActionItem, CreateTopicBatchCommandItem,
    CreateTopicsBatchAction, CreateTopicsBatchCommand, OperationId, ScenarioAction,
    TOPIC_ALREADY_EXISTS_ERROR_CODE,
};

use crate::runner_protocol::ExpectedEvent;

#[test]
fn batch_translation_is_one_ordered_command_without_expectations() {
    let client_id = ClientId::new("client-1").unwrap_or_else(|error| panic!("client: {error}"));
    let operation_id =
        OperationId::new("batch-create").unwrap_or_else(|error| panic!("operation: {error}"));
    let action = ScenarioAction::CreateTopicsBatch(CreateTopicsBatchAction {
        client_id: client_id.clone(),
        operation_id: operation_id.clone(),
        topics: vec![
            item("fresh", 2, None),
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

    assert_eq!(
        command,
        AdapterCommand::CreateTopicsBatch(CreateTopicsBatchCommand {
            client_id,
            operation_id: operation_id.clone(),
            topics: vec![command_item("fresh", 2), command_item("existing", 3)],
            timeout_ms: 500,
        })
    );
    assert!(matches!(
        expected,
        ExpectedEvent::TopicsCreationCompleted { operation_id: actual }
            if actual == operation_id
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
        expected_error_code,
    }
}

fn command_item(topic: &str, partitions: i32) -> CreateTopicBatchCommandItem {
    CreateTopicBatchCommandItem {
        topic: topic.to_owned(),
        partitions,
        replication_factor: 1,
    }
}
