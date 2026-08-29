//! Generic command recording keeps issued identities distinct from event truth.

use testlab_schema::AdapterCommand;

use super::HistoryIndex;

impl HistoryIndex {
    pub(super) fn record_generic_command(&mut self, command: &AdapterCommand) {
        if self.record_client_command(command) {
            return;
        }
        if self.record_consumer_command(command) {
            return;
        }
        match command {
            AdapterCommand::CreateClient { .. }
            | AdapterCommand::CreateConfiguredClient(_)
            | AdapterCommand::AwaitClientReady { .. }
            | AdapterCommand::ObserveClientMetrics(_)
            | AdapterCommand::CreateProducer { .. } => {
                unreachable!("client commands are indexed before generic commands")
            }
            AdapterCommand::Send { operation_id, .. } => {
                self.operations_issued.insert(operation_id.clone());
            }
            AdapterCommand::CancelProducerSend(command) => {
                self.operations_issued.insert(command.operation_id.clone());
            }
            AdapterCommand::SendBatch { operations, .. } => self.operations_issued.extend(
                operations
                    .iter()
                    .map(|operation| operation.operation_id.clone()),
            ),
            AdapterCommand::CreateAssignedConsumer { .. }
            | AdapterCommand::AssignBeginning { .. }
            | AdapterCommand::AssignBeginningBatch(_)
            | AdapterCommand::ControlAssignedConsumer(_)
            | AdapterCommand::Receive { .. }
            | AdapterCommand::GroupReceive { .. }
            | AdapterCommand::CloseAssignedConsumer { .. }
            | AdapterCommand::CreateGroupConsumer { .. }
            | AdapterCommand::ObserveGroupAssignments(_)
            | AdapterCommand::GroupReceiveSet(_)
            | AdapterCommand::ControlGroupConsumer(_)
            | AdapterCommand::ShutdownGroupConsumer(_)
            | AdapterCommand::CloseGroupConsumer { .. } => {
                unreachable!("consumer commands are indexed before generic commands")
            }
            AdapterCommand::CreateTransactionalProducer { producer_id, .. } => {
                self.transactional_producers_create_issued
                    .insert(producer_id.clone());
            }
            AdapterCommand::ExecuteTransaction { transaction_id, .. } => {
                self.transactions_execute_issued
                    .insert(transaction_id.clone());
            }
            AdapterCommand::ExecuteTransactionalTransform(command) => {
                self.transactions_execute_issued
                    .insert(command.transaction_id.clone());
            }
            AdapterCommand::FenceTransaction {
                transaction_id,
                operation,
                replacement_producer_id,
                ..
            } => {
                self.transactions_execute_issued
                    .insert(transaction_id.clone());
                self.operations_issued
                    .insert(operation.operation_id.clone());
                self.transactional_producers_create_issued
                    .insert(replacement_producer_id.clone());
            }
            AdapterCommand::CloseTransactionalProducer { producer_id } => {
                self.transactional_producers_close_issued
                    .insert(producer_id.clone());
            }
            AdapterCommand::Flush { producer_id } => {
                self.flushes_issued.insert(producer_id.clone());
            }
            AdapterCommand::CloseProducer { producer_id } => {
                self.producers_close_issued.insert(producer_id.clone());
            }
            AdapterCommand::ShutdownClient { client_id } => {
                self.clients_shutdown_issued.insert(client_id.clone());
            }
            AdapterCommand::Finish => self.finish_issued = true,
            AdapterCommand::Abort | AdapterCommand::Hello { .. } => {}
            _ => unreachable!("specialized commands are indexed before generic commands"),
        }
    }

    fn record_client_command(&mut self, command: &AdapterCommand) -> bool {
        match command {
            AdapterCommand::CreateClient { client_id } => {
                self.clients_create_issued.insert(client_id.clone());
            }
            AdapterCommand::CreateConfiguredClient(action) => {
                self.clients_create_issued.insert(action.client_id.clone());
            }
            AdapterCommand::AwaitClientReady { client_id } => {
                self.clients_ready_issued.insert(client_id.clone());
            }
            AdapterCommand::ObserveClientMetrics(command) => {
                self.client_metrics_issued
                    .insert(command.operation_id.clone(), command.client_id.clone());
            }
            AdapterCommand::CreateProducer { producer_id, .. } => {
                self.producers_create_issued.insert(producer_id.clone());
            }
            _ => return false,
        }
        true
    }
}
