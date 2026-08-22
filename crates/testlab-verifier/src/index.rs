//! History indexing separates event collection from semantic verification.

use std::collections::{BTreeMap, BTreeSet};

use testlab_schema::{
    AdapterDescriptor, ClientId, ConsumedRecord, ConsumerId, HistoryEntry, OperationId, ProducerId,
    ScenarioAction, TerminalStatus,
};

mod recording;

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
    pub(crate) committed: Option<bool>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IndexedTopicCreation {
    pub(crate) history_sequence: u64,
    pub(crate) topic: String,
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
    group_consumers_create_issued: BTreeSet<ConsumerId>,
    group_consumers_close_issued: BTreeSet<ConsumerId>,
    operations_issued: BTreeSet<OperationId>,
    topics_create_issued: BTreeSet<OperationId>,
    flushes_issued: BTreeSet<ProducerId>,
    producers_close_issued: BTreeSet<ProducerId>,
    clients_shutdown_issued: BTreeSet<ClientId>,
    finish_issued: bool,
    pub(crate) ready: Vec<(u64, AdapterDescriptor)>,
    pub(crate) accepted: BTreeMap<OperationId, Vec<u64>>,
    pub(crate) rejected: BTreeMap<OperationId, Vec<u64>>,
    pub(crate) terminals: BTreeMap<OperationId, Vec<IndexedTerminal>>,
    pub(crate) topics_created: BTreeMap<OperationId, Vec<IndexedTopicCreation>>,
    pub(crate) clients_created: BTreeMap<ClientId, Vec<u64>>,
    pub(crate) clients_ready: BTreeMap<ClientId, Vec<u64>>,
    pub(crate) producers_created: BTreeMap<ProducerId, Vec<u64>>,
    pub(crate) consumers_created: BTreeMap<ConsumerId, Vec<u64>>,
    pub(crate) assignments: BTreeMap<ConsumerId, Vec<u64>>,
    pub(crate) receives: BTreeMap<OperationId, Vec<IndexedReceive>>,
    pub(crate) consumers_closed: BTreeMap<ConsumerId, Vec<u64>>,
    pub(crate) group_consumers_created: BTreeMap<ConsumerId, Vec<u64>>,
    pub(crate) group_consumers_closed: BTreeMap<ConsumerId, Vec<u64>>,
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
            index.record(entry);
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
            ScenarioAction::CreateGroupConsumer { consumer_id, .. } => {
                self.group_consumers_create_issued.contains(consumer_id)
            }
            ScenarioAction::GroupReceive { receive_id, .. } => {
                self.receives_issued.contains(receive_id)
            }
            ScenarioAction::CloseGroupConsumer { consumer_id } => {
                self.group_consumers_close_issued.contains(consumer_id)
            }
            ScenarioAction::CreateTopic { operation_id, .. } => {
                self.topics_create_issued.contains(operation_id)
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
}

fn push<K: Ord>(map: &mut BTreeMap<K, Vec<u64>>, key: K, sequence: u64) {
    map.entry(key).or_default().push(sequence);
}
