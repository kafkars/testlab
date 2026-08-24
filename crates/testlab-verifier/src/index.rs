//! History indexing separates event collection from semantic verification.

use std::collections::{BTreeMap, BTreeSet};

use testlab_schema::{
    AdapterDescriptor, ClientId, ConsumedRecord, ConsumerId, GroupMembershipEpoch, HistoryEntry,
    OperationId, ProducerId, ScenarioAction, ShareConsumedRecord, ShareDisposition, TerminalStatus,
    TransactionDisposition,
};

mod recording;
mod share;

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
    pub(crate) group_epoch: Option<GroupMembershipEpoch>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IndexedTopicCreation {
    pub(crate) history_sequence: u64,
    pub(crate) topic: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IndexedTransactionCompletion {
    pub(crate) history_sequence: u64,
    pub(crate) disposition: TransactionDisposition,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IndexedTransactionFence {
    pub(crate) history_sequence: u64,
    pub(crate) commit_error_code: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IndexedShareReceive {
    pub(crate) history_sequence: u64,
    pub(crate) consumer_id: ConsumerId,
    pub(crate) records: Vec<ShareConsumedRecord>,
    pub(crate) member_epoch: Option<i32>,
    pub(crate) assignment_epoch: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IndexedShareAcknowledgement {
    pub(crate) history_sequence: u64,
    pub(crate) receive_id: OperationId,
    pub(crate) disposition: ShareDisposition,
    pub(crate) success: bool,
    pub(crate) delivery: Option<TerminalStatus>,
    pub(crate) code: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IndexedShareClose {
    pub(crate) history_sequence: u64,
    pub(crate) success: bool,
    pub(crate) delivery: Option<TerminalStatus>,
    pub(crate) code: Option<String>,
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
    share_consumers_create_issued: BTreeSet<ConsumerId>,
    share_receives_issued: BTreeSet<OperationId>,
    share_acknowledgements_issued: BTreeSet<OperationId>,
    share_batches_drop_issued: BTreeSet<OperationId>,
    share_consumers_close_issued: BTreeSet<ConsumerId>,
    operations_issued: BTreeSet<OperationId>,
    topics_create_issued: BTreeSet<OperationId>,
    transactional_producers_create_issued: BTreeSet<ProducerId>,
    transactions_execute_issued: BTreeSet<OperationId>,
    transactional_producers_close_issued: BTreeSet<ProducerId>,
    flushes_issued: BTreeSet<ProducerId>,
    producers_close_issued: BTreeSet<ProducerId>,
    clients_shutdown_issued: BTreeSet<ClientId>,
    finish_issued: bool,
    pub(crate) ready: Vec<(u64, AdapterDescriptor)>,
    pub(crate) accepted: BTreeMap<OperationId, Vec<u64>>,
    pub(crate) rejected: BTreeMap<OperationId, Vec<u64>>,
    pub(crate) terminals: BTreeMap<OperationId, Vec<IndexedTerminal>>,
    pub(crate) topics_created: BTreeMap<OperationId, Vec<IndexedTopicCreation>>,
    pub(crate) transactional_producers_created: BTreeMap<ProducerId, Vec<u64>>,
    pub(crate) transactions_completed: BTreeMap<OperationId, Vec<IndexedTransactionCompletion>>,
    pub(crate) transactions_fenced: BTreeMap<OperationId, Vec<IndexedTransactionFence>>,
    pub(crate) transactional_producers_closed: BTreeMap<ProducerId, Vec<u64>>,
    pub(crate) clients_created: BTreeMap<ClientId, Vec<u64>>,
    pub(crate) clients_ready: BTreeMap<ClientId, Vec<u64>>,
    pub(crate) producers_created: BTreeMap<ProducerId, Vec<u64>>,
    pub(crate) consumers_created: BTreeMap<ConsumerId, Vec<u64>>,
    pub(crate) assignments: BTreeMap<ConsumerId, Vec<u64>>,
    pub(crate) receives: BTreeMap<OperationId, Vec<IndexedReceive>>,
    pub(crate) consumers_closed: BTreeMap<ConsumerId, Vec<u64>>,
    pub(crate) group_consumers_created: BTreeMap<ConsumerId, Vec<u64>>,
    pub(crate) group_consumers_closed: BTreeMap<ConsumerId, Vec<u64>>,
    pub(crate) share_consumers_created: BTreeMap<ConsumerId, Vec<u64>>,
    pub(crate) share_receives: BTreeMap<OperationId, Vec<IndexedShareReceive>>,
    pub(crate) share_acknowledgements: BTreeMap<OperationId, Vec<IndexedShareAcknowledgement>>,
    pub(crate) share_batches_dropped: BTreeMap<OperationId, Vec<u64>>,
    pub(crate) share_consumers_closed: BTreeMap<ConsumerId, Vec<IndexedShareClose>>,
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
        if let Some(issued) = self.share_action_issued(action) {
            return issued;
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
            ScenarioAction::CreateTransactionalProducer { producer_id, .. } => self
                .transactional_producers_create_issued
                .contains(producer_id),
            ScenarioAction::ExecuteTransaction { transaction_id, .. }
            | ScenarioAction::FenceTransaction { transaction_id, .. } => {
                self.transactions_execute_issued.contains(transaction_id)
            }
            ScenarioAction::CloseTransactionalProducer { producer_id } => self
                .transactional_producers_close_issued
                .contains(producer_id),
            ScenarioAction::Flush { producer_id } => self.flushes_issued.contains(producer_id),
            ScenarioAction::CloseProducer { producer_id } => {
                self.producers_close_issued.contains(producer_id)
            }
            ScenarioAction::ShutdownClient { client_id } => {
                self.clients_shutdown_issued.contains(client_id)
            }
            ScenarioAction::SetBrokerBehavior { .. }
            | ScenarioAction::RestartBroker { .. }
            | ScenarioAction::StopBroker { .. }
            | ScenarioAction::StartBroker { .. }
            | ScenarioAction::StopPartitionLeader { .. }
            | ScenarioAction::RestorePartitionLeader { .. } => true,
            ScenarioAction::CreateShareConsumer { .. }
            | ScenarioAction::ShareReceive { .. }
            | ScenarioAction::ShareAcknowledge { .. }
            | ScenarioAction::DropShareBatch { .. }
            | ScenarioAction::CloseShareConsumer { .. } => {
                unreachable!("share actions are indexed before generic actions")
            }
        }
    }

    pub(crate) fn finish_issued(&self) -> bool {
        !self.has_harness_commands || self.finish_issued
    }
}

fn push<K: Ord>(map: &mut BTreeMap<K, Vec<u64>>, key: K, sequence: u64) {
    map.entry(key).or_default().push(sequence);
}
