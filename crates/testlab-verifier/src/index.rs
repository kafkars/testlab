//! History indexing separates event collection from semantic verification.

use std::collections::{BTreeMap, BTreeSet};

use testlab_schema::{
    AdapterCommand, AdapterDescriptor, AdapterEvent, ClientId, ConsumedRecord, ConsumerId,
    HistoryEntry, HistoryPayload, OperationId, ProducerId, ScenarioAction, TerminalStatus,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IndexedTerminal {
    pub(crate) history_sequence: u64,
    pub(crate) status: TerminalStatus,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IndexedCommandFailure {
    pub(crate) history_sequence: u64,
    pub(crate) code: String,
    pub(crate) diagnostic: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IndexedReceive {
    pub(crate) history_sequence: u64,
    pub(crate) records: Vec<ConsumedRecord>,
}

#[derive(Debug, Default)]
pub(crate) struct HistoryIndex {
    has_harness_commands: bool,
    clients_create_issued: BTreeSet<ClientId>,
    clients_ready_issued: BTreeSet<ClientId>,
    producers_create_issued: BTreeSet<ProducerId>,
    consumers_create_issued: BTreeSet<ConsumerId>,
    assignments_issued: BTreeSet<ConsumerId>,
    receives_issued: BTreeSet<OperationId>,
    consumers_close_issued: BTreeSet<ConsumerId>,
    operations_issued: BTreeSet<OperationId>,
    flushes_issued: BTreeSet<ProducerId>,
    producers_close_issued: BTreeSet<ProducerId>,
    clients_shutdown_issued: BTreeSet<ClientId>,
    finish_issued: bool,
    pub(crate) ready: Vec<(u64, AdapterDescriptor)>,
    pub(crate) accepted: BTreeMap<OperationId, Vec<u64>>,
    pub(crate) rejected: BTreeMap<OperationId, Vec<u64>>,
    pub(crate) terminals: BTreeMap<OperationId, Vec<IndexedTerminal>>,
    pub(crate) clients_created: BTreeMap<ClientId, Vec<u64>>,
    pub(crate) clients_ready: BTreeMap<ClientId, Vec<u64>>,
    pub(crate) producers_created: BTreeMap<ProducerId, Vec<u64>>,
    pub(crate) consumers_created: BTreeMap<ConsumerId, Vec<u64>>,
    pub(crate) assignments: BTreeMap<ConsumerId, Vec<u64>>,
    pub(crate) receives: BTreeMap<OperationId, Vec<IndexedReceive>>,
    pub(crate) consumers_closed: BTreeMap<ConsumerId, Vec<u64>>,
    pub(crate) flushes: BTreeMap<ProducerId, Vec<u64>>,
    pub(crate) producers_closed: BTreeMap<ProducerId, Vec<u64>>,
    pub(crate) clients_shutdown: BTreeMap<ClientId, Vec<u64>>,
    pub(crate) finished: Vec<u64>,
    pub(crate) command_failures: Vec<IndexedCommandFailure>,
}

impl HistoryIndex {
    pub(crate) fn build(history: &[HistoryEntry]) -> Self {
        let mut index = Self::default();
        for entry in history {
            match &entry.payload {
                HistoryPayload::HarnessCommand { command } => {
                    index.record_command(&command.command);
                }
                HistoryPayload::AdapterEvent { event } => {
                    match &event.event {
                        AdapterEvent::OperationAccepted { operation_id } => {
                            push(&mut index.accepted, operation_id.clone(), entry.sequence);
                        }
                        AdapterEvent::OperationRejected { operation_id, .. } => {
                            push(&mut index.rejected, operation_id.clone(), entry.sequence);
                        }
                        AdapterEvent::OperationTerminal {
                            operation_id,
                            status,
                            ..
                        } => index
                            .terminals
                            .entry(operation_id.clone())
                            .or_default()
                            .push(IndexedTerminal {
                                history_sequence: entry.sequence,
                                status: *status,
                            }),
                        AdapterEvent::ClientCreated { client_id } => push(
                            &mut index.clients_created,
                            client_id.clone(),
                            entry.sequence,
                        ),
                        AdapterEvent::ClientReady { client_id } => {
                            push(&mut index.clients_ready, client_id.clone(), entry.sequence);
                        }
                        AdapterEvent::ProducerCreated { producer_id } => push(
                            &mut index.producers_created,
                            producer_id.clone(),
                            entry.sequence,
                        ),
                        AdapterEvent::AssignedConsumerCreated { consumer_id } => push(
                            &mut index.consumers_created,
                            consumer_id.clone(),
                            entry.sequence,
                        ),
                        AdapterEvent::AssignmentCompleted { consumer_id } => {
                            push(&mut index.assignments, consumer_id.clone(), entry.sequence);
                        }
                        AdapterEvent::ReceiveCompleted {
                            receive_id,
                            records,
                        } => index.receives.entry(receive_id.clone()).or_default().push(
                            IndexedReceive {
                                history_sequence: entry.sequence,
                                records: records.clone(),
                            },
                        ),
                        AdapterEvent::AssignedConsumerClosed { consumer_id } => push(
                            &mut index.consumers_closed,
                            consumer_id.clone(),
                            entry.sequence,
                        ),
                        AdapterEvent::FlushCompleted { producer_id } => {
                            push(&mut index.flushes, producer_id.clone(), entry.sequence);
                        }
                        AdapterEvent::ProducerClosed { producer_id } => push(
                            &mut index.producers_closed,
                            producer_id.clone(),
                            entry.sequence,
                        ),
                        AdapterEvent::ClientShutdown { client_id } => push(
                            &mut index.clients_shutdown,
                            client_id.clone(),
                            entry.sequence,
                        ),
                        AdapterEvent::CommandFailed { code, diagnostic } => {
                            index.command_failures.push(IndexedCommandFailure {
                                history_sequence: entry.sequence,
                                code: code.clone(),
                                diagnostic: diagnostic.clone(),
                            });
                        }
                        AdapterEvent::Finished => index.finished.push(entry.sequence),
                        AdapterEvent::Ready { descriptor } => {
                            index.ready.push((entry.sequence, descriptor.clone()));
                        }
                        AdapterEvent::BatchCompleted { .. } | AdapterEvent::Fatal { .. } => {}
                    }
                }
                _ => {}
            }
        }
        index
    }

    pub(crate) fn action_issued(&self, action: &ScenarioAction) -> bool {
        if !self.has_harness_commands {
            return true;
        }
        match action {
            ScenarioAction::CreateClient { client_id } => {
                self.clients_create_issued.contains(client_id)
            }
            ScenarioAction::AwaitClientReady { client_id } => {
                self.clients_ready_issued.contains(client_id)
            }
            ScenarioAction::CreateProducer { producer_id, .. } => {
                self.producers_create_issued.contains(producer_id)
            }
            ScenarioAction::Send { operation_id, .. } => {
                self.operations_issued.contains(operation_id)
            }
            ScenarioAction::SendBatch { operations, .. } => operations
                .iter()
                .all(|operation| self.operations_issued.contains(&operation.operation_id)),
            ScenarioAction::CreateAssignedConsumer { consumer_id, .. } => {
                self.consumers_create_issued.contains(consumer_id)
            }
            ScenarioAction::AssignBeginning { consumer_id, .. } => {
                self.assignments_issued.contains(consumer_id)
            }
            ScenarioAction::Receive { receive_id, .. } => self.receives_issued.contains(receive_id),
            ScenarioAction::CloseAssignedConsumer { consumer_id } => {
                self.consumers_close_issued.contains(consumer_id)
            }
            ScenarioAction::Flush { producer_id } => self.flushes_issued.contains(producer_id),
            ScenarioAction::CloseProducer { producer_id } => {
                self.producers_close_issued.contains(producer_id)
            }
            ScenarioAction::ShutdownClient { client_id } => {
                self.clients_shutdown_issued.contains(client_id)
            }
            ScenarioAction::SetBrokerBehavior { .. } => true,
        }
    }

    pub(crate) fn finish_issued(&self) -> bool {
        !self.has_harness_commands || self.finish_issued
    }

    fn record_command(&mut self, command: &AdapterCommand) {
        self.has_harness_commands = true;
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
            AdapterCommand::SendBatch { operations, .. } => {
                self.operations_issued.extend(
                    operations
                        .iter()
                        .map(|operation| operation.operation_id.clone()),
                );
            }
            AdapterCommand::CreateAssignedConsumer { consumer_id, .. } => {
                self.consumers_create_issued.insert(consumer_id.clone());
            }
            AdapterCommand::AssignBeginning { consumer_id, .. } => {
                self.assignments_issued.insert(consumer_id.clone());
            }
            AdapterCommand::Receive { receive_id, .. } => {
                self.receives_issued.insert(receive_id.clone());
            }
            AdapterCommand::CloseAssignedConsumer { consumer_id } => {
                self.consumers_close_issued.insert(consumer_id.clone());
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
        }
    }
}

fn push<K: Ord>(map: &mut BTreeMap<K, Vec<u64>>, key: K, sequence: u64) {
    map.entry(key).or_default().push(sequence);
}
