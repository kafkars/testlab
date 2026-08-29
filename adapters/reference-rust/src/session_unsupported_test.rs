//! Unsupported-command tests keep capability classification explicit.

use testlab_schema::{
    AdapterCommand, AdminOffsetPosition, AlterConsumerGroupOffsetsCommand, AlterTopicConfigCommand,
    ClientId, ConsumerGroupOffsetAlteration, ConsumerGroupOffsetSelection,
    ConsumerGroupOffsetsSelection, ConsumerId, DeleteConsumerGroupOffsetsCommand,
    DescribeClassicGroupsCommand, DescribeTopicCommand, DescribeTopicConfigCommand,
    ListConsumerGroupOffsetsBatchCommand, ListConsumerGroupOffsetsCommand,
    ListConsumerGroupsOffsetsCommand, ListOffsetsCommand, ListTopicsCommand, OperationId,
};

use crate::session_unsupported::reason;

#[test]
fn read_only_admin_commands_require_admin_capability() {
    let client_id = client_id();
    let operation_id = operation_id();
    let commands = [
        AdapterCommand::DescribeTopic(DescribeTopicCommand {
            client_id: client_id.clone(),
            operation_id: operation_id.clone(),
            topic: "orders".to_owned(),
            timeout_ms: 1_000,
        }),
        AdapterCommand::ListTopics(ListTopicsCommand {
            client_id: client_id.clone(),
            operation_id: operation_id.clone(),
            include_internal: false,
            timeout_ms: 1_000,
        }),
        AdapterCommand::ListOffsets(ListOffsetsCommand {
            client_id: client_id.clone(),
            operation_id: operation_id.clone(),
            topic: "orders".to_owned(),
            partition: 0,
            position: AdminOffsetPosition::Latest,
            timeout_ms: 1_000,
        }),
        AdapterCommand::ListConsumerGroupOffsets(ListConsumerGroupOffsetsCommand {
            client_id,
            operation_id,
            group_id: "orders-group".to_owned(),
            topic: "orders".to_owned(),
            partition: 0,
            require_stable: true,
            timeout_ms: 1_000,
        }),
    ];

    for command in commands {
        assert_eq!(reason(&command), "admin capability required");
    }
}

#[test]
fn plural_admin_commands_require_admin_capability() {
    let client_id = client_id();
    let operation_id = operation_id();
    let partition = ConsumerGroupOffsetSelection {
        topic: "orders".to_owned(),
        partition: 0,
    };
    let commands = [
        AdapterCommand::ListConsumerGroupOffsetsBatch(ListConsumerGroupOffsetsBatchCommand {
            client_id: client_id.clone(),
            operation_id: operation_id.clone(),
            group_id: "alpha".to_owned(),
            require_stable: true,
            partitions: vec![partition.clone()],
            timeout_ms: 1_000,
        }),
        AdapterCommand::ListConsumerGroupsOffsets(ListConsumerGroupsOffsetsCommand {
            client_id: client_id.clone(),
            operation_id: operation_id.clone(),
            require_stable: true,
            groups: vec![ConsumerGroupOffsetsSelection {
                group_id: "alpha".to_owned(),
                partitions: vec![partition.clone()],
            }],
            timeout_ms: 1_000,
        }),
        AdapterCommand::AlterConsumerGroupOffsets(AlterConsumerGroupOffsetsCommand {
            client_id: client_id.clone(),
            operation_id: operation_id.clone(),
            group_id: "alpha".to_owned(),
            offsets: vec![ConsumerGroupOffsetAlteration {
                topic: "orders".to_owned(),
                partition: 0,
                offset: 11,
            }],
            timeout_ms: 1_000,
        }),
        AdapterCommand::DeleteConsumerGroupOffsets(DeleteConsumerGroupOffsetsCommand {
            client_id: client_id.clone(),
            operation_id: operation_id.clone(),
            group_id: "alpha".to_owned(),
            partitions: vec![partition],
            timeout_ms: 1_000,
        }),
        AdapterCommand::DescribeClassicGroups(DescribeClassicGroupsCommand {
            client_id,
            operation_id,
            group_ids: vec!["alpha".to_owned(), "beta".to_owned()],
            timeout_ms: 1_000,
        }),
    ];

    for command in commands {
        assert_eq!(reason(&command), "admin capability required");
    }
}

#[test]
fn topic_config_commands_require_admin_capability() {
    let client_id = client_id();
    let operation_id = operation_id();
    let commands = [
        AdapterCommand::DescribeTopicConfig(DescribeTopicConfigCommand {
            client_id: client_id.clone(),
            operation_id: operation_id.clone(),
            topic: "orders".to_owned(),
            config_name: "cleanup.policy".to_owned(),
            timeout_ms: 1_000,
        }),
        AdapterCommand::AlterTopicConfig(AlterTopicConfigCommand {
            client_id,
            operation_id,
            topic: "orders".to_owned(),
            config_name: "cleanup.policy".to_owned(),
            value: "compact".to_owned(),
            validate_only: false,
            timeout_ms: 1_000,
        }),
    ];

    for command in commands {
        assert_eq!(reason(&command), "admin capability required");
    }
}

#[test]
fn share_commands_require_share_consumer_capability() {
    let command = AdapterCommand::CreateShareConsumer {
        client_id: client_id(),
        consumer_id: ConsumerId::new("share-1")
            .unwrap_or_else(|error| panic!("consumer id: {error}")),
        group_id: "share-group".to_owned(),
        topic: "orders".to_owned(),
        membership_timeout_ms: 1_000,
        close_timeout_ms: 1_000,
        configuration: None,
    };

    assert_eq!(reason(&command), "share_consumer capability required");
}

fn client_id() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client id: {error}"))
}

fn operation_id() -> OperationId {
    OperationId::new("admin-read-1").unwrap_or_else(|error| panic!("operation id: {error}"))
}
