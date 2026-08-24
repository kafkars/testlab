//! Admin command translation tests preserve requested intent and event identity.

use testlab_schema::{
    AdapterCommand, AdminOffsetPosition, ClientId, CreatePartitionsAction, DescribeTopicAction,
    ListConsumerGroupOffsetsAction, ListConsumerGroupOffsetsCommand, ListOffsetsAction,
    ListTopicsAction, OperationId, ScenarioAction,
};

use crate::session_command_admin::translate;

#[test]
fn partition_creation_translation_preserves_requested_total() {
    let client_id = id(ClientId::new("client-1"));
    let operation_id = id(OperationId::new("admin-partitions-1"));
    let action = ScenarioAction::CreatePartitions(CreatePartitionsAction {
        client_id: client_id.clone(),
        operation_id: operation_id.clone(),
        topic: "orders".to_owned(),
        total_count: 3,
        timeout_ms: 20_000,
    });

    let Some((command, _)) = translate(&action) else {
        panic!("partition creation must cross the adapter boundary");
    };

    assert_eq!(
        command,
        AdapterCommand::CreatePartitions {
            client_id,
            operation_id,
            topic: "orders".to_owned(),
            total_count: 3,
            timeout_ms: 20_000,
        }
    );
}

#[test]
fn describe_translation_keeps_expectations_inside_the_harness() {
    let client_id = id(ClientId::new("client-1"));
    let operation_id = id(OperationId::new("admin-describe-1"));
    let action = ScenarioAction::DescribeTopic(DescribeTopicAction {
        client_id: client_id.clone(),
        operation_id: operation_id.clone(),
        topic: "orders".to_owned(),
        expected_partitions: vec![0, 1, 2],
        timeout_ms: 20_000,
    });

    let Some((command, _)) = translate(&action) else {
        panic!("topic description must cross the adapter boundary");
    };

    assert_eq!(
        command,
        AdapterCommand::DescribeTopic {
            client_id,
            operation_id,
            topic: "orders".to_owned(),
            timeout_ms: 20_000,
        }
    );
}

#[test]
fn topic_listing_translation_keeps_required_membership_private() {
    let client_id = id(ClientId::new("client-1"));
    let operation_id = id(OperationId::new("admin-list-topics-1"));
    let action = ScenarioAction::ListTopics(ListTopicsAction {
        client_id: client_id.clone(),
        operation_id: operation_id.clone(),
        include_internal: false,
        required_topics: vec!["orders".to_owned()],
        timeout_ms: 20_000,
    });

    let Some((command, _)) = translate(&action) else {
        panic!("topic listing must cross the adapter boundary");
    };

    assert_eq!(
        command,
        AdapterCommand::ListTopics {
            client_id,
            operation_id,
            include_internal: false,
            timeout_ms: 20_000,
        }
    );
}

#[test]
fn offset_translation_keeps_expected_result_private() {
    let client_id = id(ClientId::new("client-1"));
    let operation_id = id(OperationId::new("admin-list-offsets-1"));
    let action = ScenarioAction::ListOffsets(ListOffsetsAction {
        client_id: client_id.clone(),
        operation_id: operation_id.clone(),
        topic: "orders".to_owned(),
        partition: 2,
        position: AdminOffsetPosition::Latest,
        expected_offset: 42,
        timeout_ms: 20_000,
    });

    let Some((command, _)) = translate(&action) else {
        panic!("offset listing must cross the adapter boundary");
    };

    assert_eq!(
        command,
        AdapterCommand::ListOffsets {
            client_id,
            operation_id,
            topic: "orders".to_owned(),
            partition: 2,
            position: AdminOffsetPosition::Latest,
            timeout_ms: 20_000,
        }
    );
}

#[test]
fn group_offset_translation_keeps_expected_result_private() {
    let client_id = id(ClientId::new("client-1"));
    let operation_id = id(OperationId::new("admin-group-offsets-1"));
    let action = ScenarioAction::ListConsumerGroupOffsets(ListConsumerGroupOffsetsAction {
        client_id: client_id.clone(),
        operation_id: operation_id.clone(),
        group_id: "group-1".to_owned(),
        topic: "orders".to_owned(),
        partition: 2,
        require_stable: true,
        expected_offset: 42,
        timeout_ms: 20_000,
    });

    let Some((command, _)) = translate(&action) else {
        panic!("consumer-group offset listing must cross the adapter boundary");
    };

    assert_eq!(
        command,
        AdapterCommand::ListConsumerGroupOffsets(ListConsumerGroupOffsetsCommand {
            client_id,
            operation_id,
            group_id: "group-1".to_owned(),
            topic: "orders".to_owned(),
            partition: 2,
            require_stable: true,
            timeout_ms: 20_000,
        })
    );
}

fn id<T, E>(result: Result<T, E>) -> T
where
    E: std::fmt::Display,
{
    result.unwrap_or_else(|error| panic!("fixture id: {error}"))
}
