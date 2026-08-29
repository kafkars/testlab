//! Admin command translation tests preserve requested intent and event identity.

use testlab_schema::{
    AdapterCommand, AdminOffsetPosition, ClientId, CreatePartitionsAction, CreatePartitionsCommand,
    CreateTopicAction, CreateTopicCommand, DeleteRecordsAction, DeleteRecordsCommand,
    DescribeTopicAction, DescribeTopicCommand, ListConsumerGroupOffsetsAction,
    ListConsumerGroupOffsetsCommand, ListOffsetsAction, ListOffsetsCommand, ListTopicsAction,
    ListTopicsCommand, OperationId, ScenarioAction, TOPIC_ALREADY_EXISTS_ERROR_CODE,
};

use crate::runner_protocol::ExpectedEvent;
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
        validate_only: false,
        expected_current_count: None,
        expected_error_code: None,
        timeout_ms: 20_000,
    });

    let Some((command, _)) = translate(&action) else {
        panic!("partition creation must cross the adapter boundary");
    };

    assert_eq!(
        command,
        AdapterCommand::CreatePartitions(CreatePartitionsCommand {
            client_id,
            operation_id,
            topic: "orders".to_owned(),
            total_count: 3,
            validate_only: false,
            timeout_ms: 20_000,
        })
    );
}

#[test]
fn duplicate_creation_expectation_stays_out_of_the_wire_command() {
    let client_id = id(ClientId::new("client-1"));
    let operation_id = id(OperationId::new("admin-create-duplicate"));
    let action = ScenarioAction::CreateTopic(CreateTopicAction {
        client_id: client_id.clone(),
        operation_id: operation_id.clone(),
        topic: "orders".to_owned(),
        partitions: 2,
        replication_factor: 1,
        validate_only: false,
        expected_error_code: Some(TOPIC_ALREADY_EXISTS_ERROR_CODE.to_owned()),
        timeout_ms: 20_000,
    });

    let Some((command, _)) = translate(&action) else {
        panic!("duplicate creation must use the normal create-topic command");
    };

    assert_eq!(
        command,
        AdapterCommand::CreateTopic(CreateTopicCommand {
            client_id,
            operation_id,
            topic: "orders".to_owned(),
            partitions: 2,
            replication_factor: 1,
            validate_only: false,
            timeout_ms: 20_000,
        })
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
        expected_partitions: Some(vec![0, 1, 2]),
        expected_error_code: None,
        timeout_ms: 20_000,
    });

    let Some((command, _)) = translate(&action) else {
        panic!("topic description must cross the adapter boundary");
    };

    assert_eq!(
        command,
        AdapterCommand::DescribeTopic(DescribeTopicCommand {
            client_id,
            operation_id,
            topic: "orders".to_owned(),
            timeout_ms: 20_000,
        })
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
        AdapterCommand::ListTopics(ListTopicsCommand {
            client_id,
            operation_id,
            include_internal: false,
            timeout_ms: 20_000,
        })
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
        expected_offset: Some(42),
        expected_error_code: None,
        timeout_ms: 20_000,
    });

    let Some((command, _)) = translate(&action) else {
        panic!("offset listing must cross the adapter boundary");
    };

    assert_eq!(
        command,
        AdapterCommand::ListOffsets(ListOffsetsCommand {
            client_id,
            operation_id,
            topic: "orders".to_owned(),
            partition: 2,
            position: AdminOffsetPosition::Latest,
            timeout_ms: 20_000,
        })
    );
}

#[test]
fn earliest_offset_translation_preserves_the_public_selector() {
    let client_id = id(ClientId::new("client-1"));
    let operation_id = id(OperationId::new("admin-list-earliest-offset"));
    let action = ScenarioAction::ListOffsets(ListOffsetsAction {
        client_id: client_id.clone(),
        operation_id: operation_id.clone(),
        topic: "orders".to_owned(),
        partition: 0,
        position: AdminOffsetPosition::Earliest,
        expected_offset: Some(0),
        expected_error_code: None,
        timeout_ms: 20_000,
    });

    let Some((command, _)) = translate(&action) else {
        panic!("earliest offset listing must cross the adapter boundary");
    };
    assert_eq!(
        command,
        AdapterCommand::ListOffsets(ListOffsetsCommand {
            client_id,
            operation_id,
            topic: "orders".to_owned(),
            partition: 0,
            position: AdminOffsetPosition::Earliest,
            timeout_ms: 20_000,
        })
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

#[test]
fn delete_records_translation_keeps_expected_high_watermark_private() {
    let client_id = id(ClientId::new("client-1"));
    let operation_id = id(OperationId::new("admin-delete-records-1"));
    let action = ScenarioAction::DeleteRecords(DeleteRecordsAction {
        client_id: client_id.clone(),
        operation_id: operation_id.clone(),
        topic: "orders".to_owned(),
        partition: 2,
        before_offset: 4,
        expected_high_watermark: 7,
        timeout_ms: 20_000,
    });

    let Some((command, expected)) = translate(&action) else {
        panic!("record deletion must cross the adapter boundary");
    };
    assert_eq!(
        command,
        AdapterCommand::DeleteRecords(DeleteRecordsCommand {
            client_id,
            operation_id: operation_id.clone(),
            topic: "orders".to_owned(),
            partition: 2,
            before_offset: 4,
            timeout_ms: 20_000,
        })
    );
    assert!(matches!(
        expected,
        ExpectedEvent::RecordsDeleted {
            operation_id: actual,
            topic,
            partition: 2,
        } if actual == operation_id && topic == "orders"
    ));
}

fn id<T, E>(result: Result<T, E>) -> T
where
    E: std::fmt::Display,
{
    result.unwrap_or_else(|error| panic!("fixture id: {error}"))
}
