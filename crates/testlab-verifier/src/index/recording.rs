//! History recording classifies correlated commands and public adapter events.

use testlab_schema::{AdapterCommand, AdapterEvent, HistoryEntry, HistoryPayload};

use super::{
    HistoryIndex, IndexedCommandFailure, IndexedReceive, IndexedTerminal, IndexedTopicCreation,
    IndexedTransactionCompletion, push,
};

impl HistoryIndex {
    pub(super) fn record(&mut self, entry: &HistoryEntry) {
        match &entry.payload {
            HistoryPayload::HarnessCommand { command } => self.record_command(&command.command),
            HistoryPayload::AdapterEvent { event } => {
                self.record_event(&event.event, entry.sequence);
            }
            _ => {}
        }
    }

    fn record_event(&mut self, event: &AdapterEvent, sequence: u64) {
        if self.record_share_event(event, sequence)
            || self.record_transaction_event(event, sequence)
            || self.record_consumer_event(event, sequence)
        {
            return;
        }
        match event {
            AdapterEvent::OperationAccepted { operation_id } => {
                push(&mut self.accepted, operation_id.clone(), sequence);
            }
            AdapterEvent::OperationRejected { operation_id, .. } => {
                push(&mut self.rejected, operation_id.clone(), sequence);
            }
            AdapterEvent::OperationTerminal {
                operation_id,
                status,
                ..
            } => self
                .terminals
                .entry(operation_id.clone())
                .or_default()
                .push(IndexedTerminal {
                    history_sequence: sequence,
                    status: *status,
                }),
            AdapterEvent::ClientCreated { client_id } => {
                push(&mut self.clients_created, client_id.clone(), sequence);
            }
            AdapterEvent::ClientReady { client_id } => {
                push(&mut self.clients_ready, client_id.clone(), sequence);
            }
            AdapterEvent::ProducerCreated { producer_id } => {
                push(&mut self.producers_created, producer_id.clone(), sequence);
            }
            AdapterEvent::TopicCreated {
                operation_id,
                topic,
            } => self
                .topics_created
                .entry(operation_id.clone())
                .or_default()
                .push(IndexedTopicCreation {
                    history_sequence: sequence,
                    topic: topic.clone(),
                }),
            AdapterEvent::FlushCompleted { producer_id } => {
                push(&mut self.flushes, producer_id.clone(), sequence);
            }
            AdapterEvent::ProducerClosed { producer_id } => {
                push(&mut self.producers_closed, producer_id.clone(), sequence);
            }
            AdapterEvent::ClientShutdown { client_id } => {
                push(&mut self.clients_shutdown, client_id.clone(), sequence);
            }
            AdapterEvent::CommandFailed { code, diagnostic } => {
                self.command_failures.push(IndexedCommandFailure {
                    history_sequence: sequence,
                    code: code.clone(),
                    diagnostic: diagnostic.clone(),
                });
            }
            AdapterEvent::Finished => self.finished.push(sequence),
            AdapterEvent::Ready { descriptor } => self.ready.push((sequence, descriptor.clone())),
            AdapterEvent::BatchCompleted { .. }
            | AdapterEvent::ShareConsumerCreated { .. }
            | AdapterEvent::ShareReceiveCompleted { .. }
            | AdapterEvent::ShareAcknowledgementCompleted { .. }
            | AdapterEvent::ShareBatchDropped { .. }
            | AdapterEvent::ShareConsumerClosed { .. }
            | AdapterEvent::AssignedConsumerCreated { .. }
            | AdapterEvent::AssignmentCompleted { .. }
            | AdapterEvent::ReceiveCompleted { .. }
            | AdapterEvent::AssignedConsumerClosed { .. }
            | AdapterEvent::GroupConsumerCreated { .. }
            | AdapterEvent::GroupReceiveCompleted { .. }
            | AdapterEvent::GroupConsumerClosed { .. }
            | AdapterEvent::TransactionalProducerCreated { .. }
            | AdapterEvent::TransactionCompleted { .. }
            | AdapterEvent::TransactionFenceCompleted { .. }
            | AdapterEvent::TransactionalProducerClosed { .. }
            | AdapterEvent::Fatal { .. } => {}
        }
    }

    fn record_consumer_event(&mut self, event: &AdapterEvent, sequence: u64) -> bool {
        match event {
            AdapterEvent::AssignedConsumerCreated { consumer_id } => {
                push(&mut self.consumers_created, consumer_id.clone(), sequence);
            }
            AdapterEvent::AssignmentCompleted { consumer_id } => {
                push(&mut self.assignments, consumer_id.clone(), sequence);
            }
            AdapterEvent::ReceiveCompleted {
                receive_id,
                records,
            } => self.record_receive(receive_id, records, None, None, sequence),
            AdapterEvent::AssignedConsumerClosed { consumer_id } => {
                push(&mut self.consumers_closed, consumer_id.clone(), sequence);
            }
            AdapterEvent::GroupConsumerCreated { consumer_id } => push(
                &mut self.group_consumers_created,
                consumer_id.clone(),
                sequence,
            ),
            AdapterEvent::GroupReceiveCompleted {
                receive_id,
                records,
                committed,
                group_epoch,
            } => self.record_receive(
                receive_id,
                records,
                Some(*committed),
                *group_epoch,
                sequence,
            ),
            AdapterEvent::GroupConsumerClosed { consumer_id } => push(
                &mut self.group_consumers_closed,
                consumer_id.clone(),
                sequence,
            ),
            _ => return false,
        }
        true
    }

    fn record_transaction_event(&mut self, event: &AdapterEvent, sequence: u64) -> bool {
        match event {
            AdapterEvent::TransactionalProducerCreated { producer_id } => push(
                &mut self.transactional_producers_created,
                producer_id.clone(),
                sequence,
            ),
            AdapterEvent::TransactionCompleted {
                transaction_id,
                disposition,
            } => self
                .transactions_completed
                .entry(transaction_id.clone())
                .or_default()
                .push(IndexedTransactionCompletion {
                    history_sequence: sequence,
                    disposition: *disposition,
                }),
            AdapterEvent::TransactionFenceCompleted {
                transaction_id,
                commit_error_code,
            } => self
                .transactions_fenced
                .entry(transaction_id.clone())
                .or_default()
                .push(super::IndexedTransactionFence {
                    history_sequence: sequence,
                    commit_error_code: commit_error_code.clone(),
                }),
            AdapterEvent::TransactionalProducerClosed { producer_id } => push(
                &mut self.transactional_producers_closed,
                producer_id.clone(),
                sequence,
            ),
            _ => return false,
        }
        true
    }

    fn record_receive(
        &mut self,
        receive_id: &testlab_schema::OperationId,
        records: &[testlab_schema::ConsumedRecord],
        committed: Option<bool>,
        group_epoch: Option<testlab_schema::GroupMembershipEpoch>,
        sequence: u64,
    ) {
        self.receives
            .entry(receive_id.clone())
            .or_default()
            .push(IndexedReceive {
                history_sequence: sequence,
                records: records.to_vec(),
                committed,
                group_epoch,
            });
    }

    fn record_command(&mut self, command: &AdapterCommand) {
        self.has_harness_commands = true;
        if self.record_share_command(command) {
            return;
        }
        match command {
            AdapterCommand::CreateClient { client_id } => {
                self.clients_create_issued.insert(client_id.clone());
            }
            AdapterCommand::AwaitClientReady { client_id } => {
                self.clients_ready_issued.insert(client_id.clone());
            }
            AdapterCommand::CreateProducer { producer_id, .. } => {
                self.producers_create_issued.insert(producer_id.clone());
            }
            AdapterCommand::Send { operation_id, .. } => {
                self.operations_issued.insert(operation_id.clone());
            }
            AdapterCommand::SendBatch { operations, .. } => self.operations_issued.extend(
                operations
                    .iter()
                    .map(|operation| operation.operation_id.clone()),
            ),
            AdapterCommand::CreateAssignedConsumer { consumer_id, .. } => {
                self.consumers_create_issued.insert(consumer_id.clone());
            }
            AdapterCommand::AssignBeginning { consumer_id, .. } => {
                self.assignments_issued.insert(consumer_id.clone());
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
            AdapterCommand::CloseGroupConsumer { consumer_id } => {
                self.group_consumers_close_issued
                    .insert(consumer_id.clone());
            }
            AdapterCommand::CreateTopic { operation_id, .. } => {
                self.topics_create_issued.insert(operation_id.clone());
            }
            AdapterCommand::CreateTransactionalProducer { producer_id, .. } => {
                self.transactional_producers_create_issued
                    .insert(producer_id.clone());
            }
            AdapterCommand::ExecuteTransaction { transaction_id, .. } => {
                self.transactions_execute_issued
                    .insert(transaction_id.clone());
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
            AdapterCommand::Hello { .. } => {}
            AdapterCommand::CreateShareConsumer { .. }
            | AdapterCommand::ShareReceive { .. }
            | AdapterCommand::ShareAcknowledge { .. }
            | AdapterCommand::DropShareBatch { .. }
            | AdapterCommand::CloseShareConsumer { .. } => {
                unreachable!("share commands are indexed before generic commands")
            }
        }
    }
}
