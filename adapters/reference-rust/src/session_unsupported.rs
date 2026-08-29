//! Unsupported command classification keeps the reference adapter explicit.

use testlab_schema::AdapterCommand;

pub(super) fn reason(command: &AdapterCommand) -> &'static str {
    match command {
        AdapterCommand::CancelProducerSend(_) => "producer_cancellation capability required",
        AdapterCommand::CreateConfiguredClient(_) => "producer_configuration capability required",
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
        | AdapterCommand::CloseGroupConsumer { .. } => "consumer_groups capability required",
        AdapterCommand::CreateShareConsumer { .. }
        | AdapterCommand::ShareReceive { .. }
        | AdapterCommand::ShareAcknowledge { .. }
        | AdapterCommand::DropShareBatch { .. }
        | AdapterCommand::CloseShareConsumer { .. } => "share_consumer capability required",
        AdapterCommand::CreateTopic(_)
        | AdapterCommand::CreateTopicsBatch(_)
        | AdapterCommand::CreatePartitions(_)
        | AdapterCommand::DeleteTopic(_)
        | AdapterCommand::DescribeTopic(_)
        | AdapterCommand::ListTopics(_)
        | AdapterCommand::ListOffsets(_)
        | AdapterCommand::DeleteRecords(_)
        | AdapterCommand::DescribeTopicConfig(_)
        | AdapterCommand::AlterTopicConfig(_)
        | AdapterCommand::DescribeCluster(_)
        | AdapterCommand::ListConsumerGroups(_)
        | AdapterCommand::DescribeConsumerGroup(_)
        | AdapterCommand::ListConsumerGroupOffsets(_)
        | AdapterCommand::ListConsumerGroupOffsetsBatch(_)
        | AdapterCommand::ListConsumerGroupsOffsets(_)
        | AdapterCommand::AlterConsumerGroupOffset(_)
        | AdapterCommand::AlterConsumerGroupOffsets(_)
        | AdapterCommand::DeleteConsumerGroupOffset(_)
        | AdapterCommand::DeleteConsumerGroupOffsets(_)
        | AdapterCommand::DeleteConsumerGroup(_)
        | AdapterCommand::DescribeClassicGroups(_) => "admin capability required",
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
