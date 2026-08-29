//! History recording classifies correlated commands and public adapter events.

use testlab_schema::{
    AdapterEvent, AdapterEventEnvelope, CommandEnvelope, HistoryEntry, HistoryPayload,
};

use super::{
    HistoryIndex, IndexedCommandFailure, IndexedOperationError, IndexedTerminal,
    IndexedTransactionCompletion, push,
};

impl HistoryIndex {
    pub(super) fn record(&mut self, entry: &HistoryEntry) {
        match &entry.payload {
            HistoryPayload::HarnessCommand { command } => {
                self.record_command(command, entry.sequence);
            }
            HistoryPayload::AdapterEvent { event } => {
                self.record_event(event, entry.sequence);
            }
            HistoryPayload::BrokerStateObservation { observation: fact } => {
                self.record_state(fact, entry.sequence);
            }
            HistoryPayload::EnvironmentOperation { operation } => {
                self.environment_operations
                    .push((entry.sequence, operation.clone()));
            }
            HistoryPayload::AdversaryControl { control } => {
                self.adversary_controls
                    .push((entry.sequence, control.clone()));
            }
            HistoryPayload::AdversaryObservation { observation } => {
                self.adversary_observations
                    .push((entry.sequence, observation.clone()));
            }
            HistoryPayload::NetworkProxyControl { control } => {
                self.network_proxy_controls
                    .push((entry.sequence, control.clone()));
            }
            HistoryPayload::NetworkProxyObservation { observation } => {
                self.network_proxy_observations
                    .push((entry.sequence, observation.clone()));
            }
            _ => {}
        }
    }

    fn record_event(&mut self, envelope: &AdapterEventEnvelope, sequence: u64) {
        self.adapter_events.push((sequence, envelope.clone()));
        self.record_concurrent_event(envelope, sequence);
        let event = &envelope.event;
        if self.record_admin_event(event, sequence)
            || self.record_share_event(event, sequence)
            || self.record_transaction_event(event, sequence)
            || self.record_consumer_event(event, sequence)
        {
            return;
        }
        match event {
            AdapterEvent::OperationAccepted { operation_id } => {
                push(&mut self.accepted, operation_id.clone(), sequence);
            }
            AdapterEvent::OperationRejected { operation_id, code } => {
                push(&mut self.rejected, operation_id.clone(), sequence);
                self.record_operation_error(operation_id, code, sequence);
            }
            AdapterEvent::OperationTerminal {
                operation_id,
                status,
                code,
                offset,
            } => {
                self.terminals
                    .entry(operation_id.clone())
                    .or_default()
                    .push(IndexedTerminal {
                        history_sequence: sequence,
                        status: *status,
                        code: code.clone(),
                        offset: *offset,
                    });
                if let Some(code) = code {
                    self.record_operation_error(operation_id, code, sequence);
                }
            }
            AdapterEvent::ProducerCancellationCompleted(completion) => self
                .producer_cancellations
                .entry(completion.operation_id.clone())
                .or_default()
                .push(super::IndexedProducerCancellation {
                    history_sequence: sequence,
                    outcomes: completion.outcomes.clone(),
                }),
            AdapterEvent::ClientCreated { client_id } => {
                push(&mut self.clients_created, client_id.clone(), sequence);
            }
            AdapterEvent::ClientReady { client_id } => {
                push(&mut self.clients_ready, client_id.clone(), sequence);
            }
            AdapterEvent::ClientMetricsObserved(observation) => self
                .client_metrics
                .entry(observation.operation_id.clone())
                .or_default()
                .push(super::IndexedClientMetrics {
                    history_sequence: sequence,
                    observation: *observation.clone(),
                }),
            AdapterEvent::ProducerCreated { producer_id } => {
                push(&mut self.producers_created, producer_id.clone(), sequence);
            }
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
                    command_id: envelope.command_id.clone(),
                    code: code.clone(),
                    diagnostic: diagnostic.clone(),
                });
            }
            AdapterEvent::Finished => self.finished.push(sequence),
            AdapterEvent::Ready { descriptor } => self.ready.push((sequence, descriptor.clone())),
            _ => {}
        }
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
            AdapterEvent::TransactionalTransformCompleted(completion) => {
                self.transactions_completed
                    .entry(completion.transaction_id.clone())
                    .or_default()
                    .push(IndexedTransactionCompletion {
                        history_sequence: sequence,
                        disposition: completion.disposition,
                    });
                self.transactional_transforms
                    .entry(completion.transaction_id.clone())
                    .or_default()
                    .push(super::IndexedTransactionalTransform {
                        history_sequence: sequence,
                        completion: completion.clone(),
                    });
            }
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

    fn record_command(&mut self, envelope: &CommandEnvelope, sequence: u64) {
        let command = &envelope.command;
        self.has_harness_commands = true;
        self.command_sequences.push(sequence);
        self.commands
            .push((sequence, envelope.command_id.clone(), command.clone()));
        if self.record_concurrent_command(envelope, sequence)
            || self.record_admin_command(&envelope.command_id, command, sequence)
            || self.record_share_command(command)
        {
            return;
        }
        self.record_generic_command(command);
    }

    fn record_operation_error(
        &mut self,
        operation_id: &testlab_schema::OperationId,
        code: &str,
        sequence: u64,
    ) {
        self.operation_errors
            .entry(operation_id.clone())
            .or_default()
            .push(IndexedOperationError {
                history_sequence: sequence,
                code: code.to_owned(),
            });
    }
}
