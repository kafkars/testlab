//! Issued operation identities are derived only from recorded harness commands.

use std::collections::BTreeSet;

use testlab_schema::{
    AdapterCommand, HistoryEntry, HistoryPayload, ListConsumerGroupOffsetsCommand, OperationId,
};

#[derive(Debug, Default)]
pub(crate) struct IssuedOperations {
    pub(crate) record_operations: BTreeSet<OperationId>,
    pub(crate) group_offset_commands: Vec<ListConsumerGroupOffsetsCommand>,
}

pub(crate) fn from_history(history: &[HistoryEntry]) -> IssuedOperations {
    let mut issued = IssuedOperations::default();
    for entry in history {
        let HistoryPayload::HarnessCommand { command } = &entry.payload else {
            continue;
        };
        match &command.command {
            AdapterCommand::Send { operation_id, .. } => {
                issued.record_operations.insert(operation_id.clone());
            }
            AdapterCommand::SendBatch { operations, .. }
            | AdapterCommand::ExecuteTransaction { operations, .. } => {
                issued.record_operations.extend(
                    operations
                        .iter()
                        .map(|operation| operation.operation_id.clone()),
                );
            }
            AdapterCommand::FenceTransaction { operation, .. } => {
                issued
                    .record_operations
                    .insert(operation.operation_id.clone());
            }
            AdapterCommand::StartConcurrentActors(command) => {
                issued
                    .record_operations
                    .extend(command.actors.iter().filter_map(|actor| match actor {
                        testlab_schema::ConcurrentActorCommand::ProducerSend {
                            operation_id,
                            ..
                        } => Some(operation_id.clone()),
                        testlab_schema::ConcurrentActorCommand::AssignedReceive { .. } => None,
                    }));
            }
            AdapterCommand::ListConsumerGroupOffsets(action) => {
                issued.group_offset_commands.push(action.clone());
            }
            _ => {}
        }
    }
    issued
}
