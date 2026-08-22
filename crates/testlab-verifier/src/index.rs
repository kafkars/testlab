//! History indexing separates event collection from semantic verification.

use std::collections::BTreeMap;

use testlab_schema::{
    AdapterDescriptor, AdapterEvent, ClientId, HistoryEntry, HistoryPayload, OperationId,
    ProducerId, TerminalStatus,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IndexedTerminal {
    pub(crate) history_sequence: u64,
    pub(crate) status: TerminalStatus,
}

#[derive(Debug, Default)]
pub(crate) struct HistoryIndex {
    pub(crate) ready: Vec<(u64, AdapterDescriptor)>,
    pub(crate) accepted: BTreeMap<OperationId, Vec<u64>>,
    pub(crate) rejected: BTreeMap<OperationId, Vec<u64>>,
    pub(crate) terminals: BTreeMap<OperationId, Vec<IndexedTerminal>>,
    pub(crate) clients_created: BTreeMap<ClientId, Vec<u64>>,
    pub(crate) producers_created: BTreeMap<ProducerId, Vec<u64>>,
    pub(crate) flushes: BTreeMap<ProducerId, Vec<u64>>,
    pub(crate) producers_closed: BTreeMap<ProducerId, Vec<u64>>,
    pub(crate) clients_shutdown: BTreeMap<ClientId, Vec<u64>>,
    pub(crate) finished: Vec<u64>,
}

impl HistoryIndex {
    pub(crate) fn build(history: &[HistoryEntry]) -> Self {
        let mut index = Self::default();
        for entry in history {
            let HistoryPayload::AdapterEvent { event } = &entry.payload else {
                continue;
            };
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
                AdapterEvent::ProducerCreated { producer_id } => push(
                    &mut index.producers_created,
                    producer_id.clone(),
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
                AdapterEvent::Finished => index.finished.push(entry.sequence),
                AdapterEvent::Ready { descriptor } => {
                    index.ready.push((entry.sequence, descriptor.clone()));
                }
                AdapterEvent::Fatal { .. } => {}
            }
        }
        index
    }
}

fn push<K: Ord>(map: &mut BTreeMap<K, Vec<u64>>, key: K, sequence: u64) {
    map.entry(key).or_default().push(sequence);
}
