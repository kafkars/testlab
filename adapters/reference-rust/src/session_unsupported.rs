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
        AdapterCommand::CreateTopic { .. } | AdapterCommand::CreatePartitions { .. } => {
            "admin capability required"
        }
        AdapterCommand::CreateTransactionalProducer { .. }
        | AdapterCommand::ExecuteTransaction { .. }
        | AdapterCommand::FenceTransaction { .. }
        | AdapterCommand::CloseTransactionalProducer { .. } => "transactions capability required",
        _ => "unsupported command",
    }
}
