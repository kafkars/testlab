//! Unsupported command classification keeps the reference adapter explicit.

use testlab_schema::AdapterCommand;

pub(super) fn reason(command: &AdapterCommand) -> &'static str {
    match command {
        AdapterCommand::CreateAssignedConsumer { .. }
        | AdapterCommand::AssignBeginning { .. }
        | AdapterCommand::Receive { .. }
        | AdapterCommand::CloseAssignedConsumer { .. } => "assigned_consumer capability required",
        AdapterCommand::CreateGroupConsumer { .. }
        | AdapterCommand::GroupReceive { .. }
        | AdapterCommand::CloseGroupConsumer { .. } => "consumer_groups capability required",
        AdapterCommand::CreateShareConsumer { .. }
        | AdapterCommand::ShareReceive { .. }
        | AdapterCommand::ShareAcknowledge { .. }
        | AdapterCommand::DropShareBatch { .. }
        | AdapterCommand::CloseShareConsumer { .. } => "share_consumer capability required",
        AdapterCommand::CreateTopic { .. }
        | AdapterCommand::CreatePartitions { .. }
        | AdapterCommand::DescribeTopic { .. }
        | AdapterCommand::ListTopics { .. }
        | AdapterCommand::ListOffsets { .. } => "admin capability required",
        AdapterCommand::CreateTransactionalProducer { .. }
        | AdapterCommand::ExecuteTransaction { .. }
        | AdapterCommand::FenceTransaction { .. }
        | AdapterCommand::CloseTransactionalProducer { .. } => "transactions capability required",
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
