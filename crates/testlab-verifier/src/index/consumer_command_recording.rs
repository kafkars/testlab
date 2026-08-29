//! Consumer command recording indexes assigned and group consumer operation identities.

use testlab_schema::AdapterCommand;

use super::HistoryIndex;

impl HistoryIndex {
    pub(super) fn record_consumer_command(&mut self, command: &AdapterCommand) -> bool {
        match command {
            AdapterCommand::CreateAssignedConsumer { consumer_id, .. } => {
                self.consumers_create_issued.insert(consumer_id.clone());
            }
            AdapterCommand::AssignBeginning { consumer_id, .. } => {
                self.assignments_issued.insert(consumer_id.clone());
            }
            AdapterCommand::AssignBeginningBatch(action) => {
                self.assignments_issued.insert(action.consumer_id.clone());
            }
            AdapterCommand::ControlAssignedConsumer(command) => {
                self.assigned_controls_issued
                    .insert(command.operation_id.clone(), command.clone());
            }
            AdapterCommand::Receive { receive_id, .. }
            | AdapterCommand::GroupReceive { receive_id, .. } => {
                self.receives_issued.insert(receive_id.clone());
            }
            AdapterCommand::CloseAssignedConsumer { consumer_id } => {
                self.consumers_close_issued.insert(consumer_id.clone());
            }
            AdapterCommand::CreateGroupConsumer { consumer_id, .. } => {
                self.group_consumers_create_issued
                    .insert(consumer_id.clone());
            }
            AdapterCommand::ObserveGroupAssignments(command) => {
                self.group_assignments_issued
                    .insert(command.operation_id.clone());
            }
            AdapterCommand::GroupReceiveSet(command) => {
                self.group_receive_sets_issued
                    .insert(command.receive_id.clone());
            }
            AdapterCommand::ControlGroupConsumer(command) => {
                self.group_controls_issued
                    .insert(command.operation_id.clone(), command.clone());
            }
            AdapterCommand::ShutdownGroupConsumer(command) => {
                self.group_shutdowns_issued
                    .insert(command.operation_id.clone());
            }
            AdapterCommand::CloseGroupConsumer { consumer_id } => {
                self.group_consumers_close_issued
                    .insert(consumer_id.clone());
            }
            _ => return false,
        }
        true
    }
}
