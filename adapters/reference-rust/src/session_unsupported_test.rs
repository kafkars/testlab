//! Unsupported-command tests keep capability classification explicit.

use testlab_schema::{
    AdapterCommand, AdminOffsetSelector, AlterConsumerGroupOffsetsCommand, AlterTopicConfigCommand,
    AlterTopicConfigsCommand, AssignedRecordConversionMethod, AssignedRecordTransferCommand,
    ClientId, ConsumerGroupOffsetAlteration, ConsumerGroupOffsetSelection,
    ConsumerGroupOffsetsSelection, ConsumerId, DeleteConsumerGroupOffsetsCommand,
    DescribeClassicGroupsCommand, DescribeTopicCommand, DescribeTopicConfigCommand,
    DescribeTopicConfigsCommand, ListConfigResourcesCommand, ListConsumerGroupOffsetsBatchCommand,
    ListConsumerGroupOffsetsCommand, ListConsumerGroupsOffsetsCommand, ListOffsetsCommand,
    ListTopicsCommand, OperationId, ProducerId, TopicConfigAlteration, TopicConfigSelection,
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
            api: testlab_schema::TopicDescriptionApi::Metadata,
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
            position: AdminOffsetSelector::Latest,
            timestamp_millis: None,
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
        AdapterCommand::ListConfigResources(ListConfigResourcesCommand {
            client_id: client_id.clone(),
            operation_id: operation_id.clone(),
            api: testlab_schema::ConfigResourceListingApi::Resource,
            timeout_ms: 1_000,
        }),
        AdapterCommand::DescribeTopicConfigs(DescribeTopicConfigsCommand {
            client_id: client_id.clone(),
            operation_id: operation_id.clone(),
            api: testlab_schema::TopicConfigApi::Topic,
            include_synonyms: false,
            include_documentation: false,
            topics: vec![TopicConfigSelection {
                topic: "orders".to_owned(),
                config_name: "cleanup.policy".to_owned(),
            }],
            timeout_ms: 1_000,
        }),
        AdapterCommand::AlterTopicConfigs(AlterTopicConfigsCommand {
            client_id: client_id.clone(),
            operation_id: operation_id.clone(),
            api: testlab_schema::TopicConfigMutationApi::Topic,
            topics: vec![TopicConfigAlteration {
                topic: "orders".to_owned(),
                config_name: "cleanup.policy".to_owned(),
                method: testlab_schema::TopicConfigMutationMethod::Set,
                value: Some("compact".to_owned()),
            }],
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
        topics: vec!["orders".to_owned()],
        rack: None,
        membership_timeout_ms: 1_000,
        close_timeout_ms: 1_000,
        configuration: None,
    };

    assert_eq!(reason(&command), "share_consumer capability required");
}

#[test]
fn expected_cluster_identity_requires_its_capability() {
    let command = AdapterCommand::CreateClient(testlab_schema::CreateClientCommand {
        client_id: client_id(),
        expected_cluster_id: Some("cluster-a".to_owned()),
    });

    assert_eq!(
        reason(&command),
        "expected_cluster_identity capability required"
    );
}

#[test]
fn owned_record_transfer_requires_its_exact_capability() {
    let command = AdapterCommand::TransferAssignedRecord(AssignedRecordTransferCommand {
        consumer_id: ConsumerId::new("consumer-1")
            .unwrap_or_else(|error| panic!("consumer id: {error}")),
        producer_id: ProducerId::new("producer-1")
            .unwrap_or_else(|error| panic!("producer id: {error}")),
        operation_id: operation_id(),
        method: AssignedRecordConversionMethod::IntoOwnedBatch,
        target_topic: "destination".to_owned(),
        target_partition: 0,
        timeout_ms: 1_000,
    });
    assert_eq!(
        reason(&command),
        "assigned_consumer_record_transfer capability required"
    );
}

fn client_id() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client id: {error}"))
}

fn operation_id() -> OperationId {
    OperationId::new("admin-read-1").unwrap_or_else(|error| panic!("operation id: {error}"))
}
