use crate::runner_protocol::ExpectedEvent;
use testlab_schema::{
    AdapterCommand, AlterConsumerGroupOffsetCommand, CreatePartitionsCommand, CreateTopicCommand,
    DeleteConsumerGroupCommand, DeleteConsumerGroupOffsetCommand, DeleteTopicCommand,
    DescribeClusterCommand, DescribeConsumerGroupCommand, DescribeFeaturesCommand,
    DescribeLogDirsCommand, DescribeMetadataQuorumCommand, DescribeProducersCommand,
    DescribeTopicCommand, ListConsumerGroupOffsetsCommand, ListConsumerGroupsCommand,
    ListOffsetsCommand, ListTopicsCommand, ScenarioAction, ValidateFeatureUpdatesCommand,
};
pub(crate) fn translate(action: &ScenarioAction) -> Option<(AdapterCommand, ExpectedEvent)> {
    crate::session_command_admin_broker_unregistration::translate(action)
        .or_else(|| crate::session_command_admin_streams_group::translate(action))
        .or_else(|| crate::session_command_admin_delegation_token::translate(action))
        .or_else(|| crate::session_command_admin_user_scram::translate(action))
        .or_else(|| crate::session_command_admin_replica_log_dirs::translate(action))
        .or_else(|| crate::session_command_admin_leader_election::translate(action))
        .or_else(|| crate::session_command_admin_partition_reassignments::translate(action))
        .or_else(|| crate::session_command_admin_transactions::translate(action))
        .or_else(|| crate::session_command_admin_share_group::translate(action))
        .or_else(|| crate::session_command_admin_client_quota::translate(action))
        .or_else(|| crate::session_command_admin_acl::translate(action))
        .or_else(|| crate::session_command_admin_batch::translate(action))
        .or_else(|| translate_topic(action))
        .or_else(|| crate::session_command_admin_records::translate(action))
        .or_else(|| crate::session_command_admin_config::translate(action))
        .or_else(|| crate::session_command_admin_group_batch::translate(action))
        .or_else(|| translate_group(action))
}

fn translate_topic(action: &ScenarioAction) -> Option<(AdapterCommand, ExpectedEvent)> {
    Some(match action {
        ScenarioAction::CreateTopic(action) => (
            AdapterCommand::CreateTopic(CreateTopicCommand {
                client_id: action.client_id.clone(),
                operation_id: action.operation_id.clone(),
                topic: action.topic.clone(),
                partitions: action.partitions,
                replication_factor: action.replication_factor,
                replica_assignments: action.replica_assignments.clone(),
                validate_only: action.validate_only,
                timeout_ms: action.timeout_ms,
            }),
            topic_creation_event(action),
        ),
        ScenarioAction::CreatePartitions(action) => (
            AdapterCommand::CreatePartitions(CreatePartitionsCommand {
                client_id: action.client_id.clone(),
                operation_id: action.operation_id.clone(),
                topic: action.topic.clone(),
                total_count: action.total_count,
                replica_assignments: action.replica_assignments.clone(),
                validate_only: action.validate_only,
                timeout_ms: action.timeout_ms,
            }),
            partition_creation_event(action),
        ),
        ScenarioAction::DeleteTopic(action) => (
            AdapterCommand::DeleteTopic(DeleteTopicCommand {
                client_id: action.client_id.clone(),
                operation_id: action.operation_id.clone(),
                topic: action.topic.clone(),
                timeout_ms: action.timeout_ms,
            }),
            ExpectedEvent::TopicDeleted {
                operation_id: action.operation_id.clone(),
                topic: action.topic.clone(),
            },
        ),
        ScenarioAction::DescribeTopic(action) => (
            AdapterCommand::DescribeTopic(DescribeTopicCommand {
                client_id: action.client_id.clone(),
                operation_id: action.operation_id.clone(),
                topic: action.topic.clone(),
                api: action.api,
                pagination: action.pagination,
                timeout_ms: action.timeout_ms,
            }),
            ExpectedEvent::TopicDescribed {
                operation_id: action.operation_id.clone(),
                topic: action.topic.clone(),
            },
        ),
        ScenarioAction::ListTopics(action) => (
            AdapterCommand::ListTopics(ListTopicsCommand {
                client_id: action.client_id.clone(),
                operation_id: action.operation_id.clone(),
                include_internal: action.include_internal,
                include_authorized_operations: action.include_authorized_operations,
                timeout_ms: action.timeout_ms,
            }),
            ExpectedEvent::TopicsListed {
                operation_id: action.operation_id.clone(),
            },
        ),
        ScenarioAction::ListOffsets(action) => (
            AdapterCommand::ListOffsets(ListOffsetsCommand {
                client_id: action.client_id.clone(),
                operation_id: action.operation_id.clone(),
                topic: action.topic.clone(),
                partition: action.partition,
                position: action.position,
                read_isolation: action.read_isolation,
                timestamp_millis: action.timestamp_millis,
                timeout_ms: action.timeout_ms,
            }),
            ExpectedEvent::OffsetListed {
                operation_id: action.operation_id.clone(),
                topic: action.topic.clone(),
                partition: action.partition,
            },
        ),
        ScenarioAction::DescribeCluster(action) => (
            AdapterCommand::DescribeCluster(DescribeClusterCommand {
                client_id: action.client_id.clone(),
                operation_id: action.operation_id.clone(),
                include_fenced_brokers: action.include_fenced_brokers,
                include_authorized_operations: action.include_authorized_operations,
                timeout_ms: action.timeout_ms,
            }),
            ExpectedEvent::ClusterDescribed(action.operation_id.clone()),
        ),
        ScenarioAction::DescribeFeatures(action) => (
            AdapterCommand::DescribeFeatures(DescribeFeaturesCommand {
                client_id: action.client_id.clone(),
                operation_id: action.operation_id.clone(),
                timeout_ms: action.timeout_ms,
            }),
            ExpectedEvent::FeaturesDescribed(action.operation_id.clone()),
        ),
        ScenarioAction::ValidateFeatureUpdates(action) => (
            AdapterCommand::ValidateFeatureUpdates(ValidateFeatureUpdatesCommand {
                client_id: action.client_id.clone(),
                operation_id: action.operation_id.clone(),
                updates: action.updates.clone(),
                timeout_ms: action.timeout_ms,
            }),
            ExpectedEvent::FeatureUpdatesValidated(action.operation_id.clone()),
        ),
        ScenarioAction::DescribeMetadataQuorum(action) => (
            AdapterCommand::DescribeMetadataQuorum(DescribeMetadataQuorumCommand {
                client_id: action.client_id.clone(),
                operation_id: action.operation_id.clone(),
                timeout_ms: action.timeout_ms,
            }),
            ExpectedEvent::MetadataQuorumDescribed(action.operation_id.clone()),
        ),
        ScenarioAction::DescribeProducers(action) => (
            AdapterCommand::DescribeProducers(DescribeProducersCommand {
                client_id: action.client_id.clone(),
                operation_id: action.operation_id.clone(),
                topic: action.topic.clone(),
                partition: action.partition,
                timeout_ms: action.timeout_ms,
            }),
            ExpectedEvent::ProducerStatesDescribed {
                operation_id: action.operation_id.clone(),
                topic: action.topic.clone(),
                partition: action.partition,
            },
        ),
        ScenarioAction::DescribeLogDirs(action) => (
            AdapterCommand::DescribeLogDirs(DescribeLogDirsCommand {
                client_id: action.client_id.clone(),
                operation_id: action.operation_id.clone(),
                topic: action.topic.clone(),
                partition: action.partition,
                timeout_ms: action.timeout_ms,
            }),
            ExpectedEvent::LogDirsDescribed {
                operation_id: action.operation_id.clone(),
                topic: action.topic.clone(),
                partition: action.partition,
            },
        ),
        _ => return None,
    })
}

fn topic_creation_event(action: &testlab_schema::CreateTopicAction) -> ExpectedEvent {
    if action.validate_only {
        ExpectedEvent::TopicCreationValidated {
            operation_id: action.operation_id.clone(),
            topic: action.topic.clone(),
        }
    } else {
        ExpectedEvent::TopicCreated {
            operation_id: action.operation_id.clone(),
            topic: action.topic.clone(),
        }
    }
}

fn partition_creation_event(action: &testlab_schema::CreatePartitionsAction) -> ExpectedEvent {
    if action.validate_only {
        ExpectedEvent::TopicPartitionIncreaseValidated {
            operation_id: action.operation_id.clone(),
            topic: action.topic.clone(),
        }
    } else {
        ExpectedEvent::TopicPartitionsCreated {
            operation_id: action.operation_id.clone(),
            topic: action.topic.clone(),
        }
    }
}

fn translate_group(action: &ScenarioAction) -> Option<(AdapterCommand, ExpectedEvent)> {
    Some(match action {
        ScenarioAction::ListConsumerGroups(action) => (
            AdapterCommand::ListConsumerGroups(ListConsumerGroupsCommand {
                client_id: action.client_id.clone(),
                operation_id: action.operation_id.clone(),
                api: action.api,
                state_filters: action.state_filters.clone(),
                group_type_filters: action.group_type_filters.clone(),
                protocol_type_filters: action.protocol_type_filters.clone(),
                timeout_ms: action.timeout_ms,
            }),
            ExpectedEvent::ConsumerGroupsListed {
                operation_id: action.operation_id.clone(),
            },
        ),
        ScenarioAction::DescribeConsumerGroup(action) => (
            AdapterCommand::DescribeConsumerGroup(DescribeConsumerGroupCommand {
                client_id: action.client_id.clone(),
                operation_id: action.operation_id.clone(),
                group_id: action.group_id.clone(),
                include_authorized_operations: action.include_authorized_operations,
                timeout_ms: action.timeout_ms,
            }),
            ExpectedEvent::ConsumerGroupDescribed {
                operation_id: action.operation_id.clone(),
                group_id: action.group_id.clone(),
            },
        ),
        ScenarioAction::ListConsumerGroupOffsets(action) => (
            AdapterCommand::ListConsumerGroupOffsets(ListConsumerGroupOffsetsCommand {
                client_id: action.client_id.clone(),
                operation_id: action.operation_id.clone(),
                group_id: action.group_id.clone(),
                topic: action.topic.clone(),
                partition: action.partition,
                require_stable: action.require_stable,
                timeout_ms: action.timeout_ms,
            }),
            ExpectedEvent::ConsumerGroupOffsetListed {
                operation_id: action.operation_id.clone(),
                group_id: action.group_id.clone(),
                topic: action.topic.clone(),
                partition: action.partition,
            },
        ),
        ScenarioAction::AlterConsumerGroupOffset(action) => (
            AdapterCommand::AlterConsumerGroupOffset(AlterConsumerGroupOffsetCommand {
                client_id: action.client_id.clone(),
                operation_id: action.operation_id.clone(),
                group_id: action.group_id.clone(),
                topic: action.topic.clone(),
                partition: action.partition,
                offset: action.offset,
                timeout_ms: action.timeout_ms,
            }),
            ExpectedEvent::ConsumerGroupOffsetAltered {
                operation_id: action.operation_id.clone(),
                group_id: action.group_id.clone(),
                topic: action.topic.clone(),
                partition: action.partition,
            },
        ),
        ScenarioAction::DeleteConsumerGroupOffset(action) => (
            AdapterCommand::DeleteConsumerGroupOffset(DeleteConsumerGroupOffsetCommand {
                client_id: action.client_id.clone(),
                operation_id: action.operation_id.clone(),
                group_id: action.group_id.clone(),
                topic: action.topic.clone(),
                partition: action.partition,
                timeout_ms: action.timeout_ms,
            }),
            ExpectedEvent::ConsumerGroupOffsetDeleted {
                operation_id: action.operation_id.clone(),
                group_id: action.group_id.clone(),
                topic: action.topic.clone(),
                partition: action.partition,
            },
        ),
        ScenarioAction::DeleteConsumerGroup(action) => (
            AdapterCommand::DeleteConsumerGroup(DeleteConsumerGroupCommand {
                client_id: action.client_id.clone(),
                operation_id: action.operation_id.clone(),
                group_id: action.group_id.clone(),
                timeout_ms: action.timeout_ms,
            }),
            ExpectedEvent::ConsumerGroupDeleted {
                operation_id: action.operation_id.clone(),
                group_id: action.group_id.clone(),
            },
        ),
        _ => return None,
    })
}
