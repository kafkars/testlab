//! Unsupported command classification keeps the reference adapter explicit.

use testlab_schema::AdapterCommand;

pub(super) fn reason(command: &AdapterCommand) -> &'static str {
    match command {
        AdapterCommand::CancelProducerSend(_) => "producer_cancellation capability required",
        AdapterCommand::CreateConfiguredClient(_) => "producer_configuration capability required",
        AdapterCommand::CreateAssignedConsumerClient(_) => {
            "assigned_consumer_configuration capability required"
        }
        AdapterCommand::ObserveClientMetrics(_) => "client_metrics capability required",
        AdapterCommand::CreateAssignedConsumer { .. }
        | AdapterCommand::AssignBeginning { .. }
        | AdapterCommand::AssignBeginningBatch(_)
        | AdapterCommand::ControlAssignedConsumer(_)
        | AdapterCommand::Receive { .. }
        | AdapterCommand::CloseAssignedConsumer { .. } => "assigned_consumer capability required",
        AdapterCommand::CreateGroupConsumer { .. }
        | AdapterCommand::GroupReceive { .. }
        | AdapterCommand::ObserveGroupAssignments(_)
        | AdapterCommand::GroupReceiveSet(_)
        | AdapterCommand::ControlGroupConsumer(_)
        | AdapterCommand::ShutdownGroupConsumer(_)
        | AdapterCommand::CloseGroupConsumer { .. }
        | AdapterCommand::AbandonGroupConsumer(_) => "consumer_groups capability required",
        AdapterCommand::CreateShareConsumer { .. }
        | AdapterCommand::ShareReceive { .. }
        | AdapterCommand::ShareAcknowledge { .. }
        | AdapterCommand::DropShareBatch { .. }
        | AdapterCommand::CloseShareConsumer { .. } => "share_consumer capability required",
        AdapterCommand::CreateTopic(_)
        | AdapterCommand::CreateTopicsBatch(_)
        | AdapterCommand::CreatePartitions(_)
        | AdapterCommand::DeleteTopic(_)
        | AdapterCommand::DeleteTopics(_)
        | AdapterCommand::DescribeTopic(_)
        | AdapterCommand::DescribeTopics(_)
        | AdapterCommand::ListTopics(_)
        | AdapterCommand::ListConfigResources(_)
        | AdapterCommand::ListOffsets(_)
        | AdapterCommand::ListOffsetsBatch(_)
        | AdapterCommand::DeleteRecords(_)
        | AdapterCommand::DeleteRecordsBatch(_)
        | AdapterCommand::DescribeTopicConfig(_)
        | AdapterCommand::DescribeTopicConfigs(_)
        | AdapterCommand::AlterTopicConfigs(_)
        | AdapterCommand::AlterTopicConfig(_)
        | AdapterCommand::DescribeCluster(_)
        | AdapterCommand::DescribeFeatures(_)
        | AdapterCommand::ValidateFeatureUpdates(_)
        | AdapterCommand::DescribeProducers(_)
        | AdapterCommand::DescribeLogDirs(_)
        | AdapterCommand::DescribeReplicaLogDirs(_)
        | AdapterCommand::AlterReplicaLogDirs(_)
        | AdapterCommand::DescribeMetadataQuorum(_)
        | AdapterCommand::ListTransactions(_)
        | AdapterCommand::DescribeTransactions(_)
        | AdapterCommand::FenceProducers(_)
        | AdapterCommand::AlterPartitionReassignments(_)
        | AdapterCommand::ListPartitionReassignments(_)
        | AdapterCommand::ElectLeaders(_)
        | AdapterCommand::ListConsumerGroups(_)
        | AdapterCommand::DescribeConsumerGroup(_)
        | AdapterCommand::DescribeConsumerGroups(_)
        | AdapterCommand::DescribeShareGroup(_)
        | AdapterCommand::DescribeShareGroups(_)
        | AdapterCommand::ListShareGroupOffsets(_)
        | AdapterCommand::ListShareGroupsOffsets(_)
        | AdapterCommand::AlterShareGroupOffsets(_)
        | AdapterCommand::DeleteShareGroupOffsets(_)
        | AdapterCommand::DeleteShareGroups(_)
        | AdapterCommand::ListConsumerGroupOffsets(_)
        | AdapterCommand::ListConsumerGroupOffsetsBatch(_)
        | AdapterCommand::ListConsumerGroupsOffsets(_)
        | AdapterCommand::AlterConsumerGroupOffset(_)
        | AdapterCommand::AlterConsumerGroupOffsets(_)
        | AdapterCommand::DeleteConsumerGroupOffset(_)
        | AdapterCommand::DeleteConsumerGroupOffsets(_)
        | AdapterCommand::DeleteConsumerGroup(_)
        | AdapterCommand::DeleteConsumerGroups(_)
        | AdapterCommand::RemoveConsumerGroupMembers(_)
        | AdapterCommand::DescribeClassicGroups(_)
        | AdapterCommand::CreateAcls(_)
        | AdapterCommand::DescribeAcls(_)
        | AdapterCommand::DeleteAcls(_)
        | AdapterCommand::AlterClientQuota(_)
        | AdapterCommand::DescribeClientQuota(_)
        | AdapterCommand::AlterUserScramCredential(_)
        | AdapterCommand::DescribeUserScramCredential(_) => "admin capability required",
        AdapterCommand::CreateTransactionalProducer { .. }
        | AdapterCommand::ExecuteTransaction { .. }
        | AdapterCommand::ExecuteTransactionalTransform(_)
        | AdapterCommand::FenceTransaction { .. }
        | AdapterCommand::CloseTransactionalProducer { .. } => "transactions capability required",
        AdapterCommand::StartConcurrentActors(_) | AdapterCommand::JoinConcurrentActors { .. } => {
            "concurrent_actors capability required"
        }
        AdapterCommand::Hello { .. }
        | AdapterCommand::CreateClient { .. }
        | AdapterCommand::AwaitClientReady { .. }
        | AdapterCommand::CreateProducer { .. }
        | AdapterCommand::Send { .. }
        | AdapterCommand::SendBatch { .. }
        | AdapterCommand::Flush { .. }
        | AdapterCommand::CloseProducer { .. }
        | AdapterCommand::ShutdownClient { .. }
        | AdapterCommand::Finish
        | AdapterCommand::Abort => "unsupported command",
    }
}
