//! Issued record operations are derived only from recorded harness commands.

use std::collections::BTreeSet;

use testlab_schema::{AdapterCommand, HistoryEntry, HistoryPayload, OperationId};

pub(crate) fn from_history(history: &[HistoryEntry]) -> BTreeSet<OperationId> {
    let mut issued = BTreeSet::new();
    for entry in history {
        let HistoryPayload::HarnessCommand { command } = &entry.payload else {
            continue;
        };
        match &command.command {
            AdapterCommand::Send { operation_id, .. } => {
                issued.insert(operation_id.clone());
            }
            AdapterCommand::SendBatch { operations, .. }
            | AdapterCommand::ExecuteTransaction { operations, .. } => {
                issued.extend(
                    operations
                        .iter()
                        .map(|operation| operation.operation_id.clone()),
                );
            }
            AdapterCommand::FenceTransaction { operation, .. } => {
                issued.insert(operation.operation_id.clone());
            }
            _ => {}
        }
    }
    issued
}
