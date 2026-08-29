//! History indexing separates event collection from semantic verification.

use std::collections::{BTreeMap, BTreeSet};

use testlab_schema::{
    AdapterDescriptor, ClientId, CommandId, ConsumedRecord, ConsumerId, EnvironmentOperation,
    GroupAssignmentsObservation, GroupMembershipEpoch, GroupReceiveSetCompletion, HistoryEntry,
    OperationId, ProducerId, ShareConsumedRecord, ShareDisposition, TerminalStatus,
    TransactionDisposition, TransactionalTransformCompletion,
};

mod admin_batch_command_match;
mod admin_command_match;
mod admin_config_command_match;
mod admin_delete_records_command_match;
pub(crate) mod admin_group_batch;
mod admin_recording;
mod admin_state_recording;
mod admin_types;
mod admin_validation;
mod concurrent;
mod consumer_command_recording;
mod consumer_recording;
mod generic_command_recording;
mod issued;
mod recording;
mod share;

pub(crate) use admin_types::{
    IndexedAdminGroupCompletion, IndexedAdminGroupOffsetCompletion, IndexedAdminTopicCompletion,
    IndexedAdminTopicConfigCompletion, IndexedAdminTopicsCreationBatch, IndexedClusterDescription,
    IndexedClusterObservation, IndexedConsumerGroupDescription, IndexedConsumerGroupObservation,
    IndexedConsumerGroupOffset, IndexedConsumerGroupOffsetObservation, IndexedConsumerGroupsList,
    IndexedOffsetList, IndexedPartitionOffsetsObservation, IndexedRecordsDeleted,
    IndexedTopicConfigDescription, IndexedTopicConfigObservation, IndexedTopicDescription,
    IndexedTopicObservation, IndexedTopicsList,
};
pub(crate) use concurrent::{
    ConcurrentPublicEventKind, IndexedConcurrentActorCompletion, IndexedConcurrentBoundary,
    IndexedConcurrentJoin, IndexedConcurrentPublicEvent, IndexedConcurrentStart,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IndexedTerminal {
    pub(crate) history_sequence: u64,
    pub(crate) status: TerminalStatus,
    pub(crate) code: Option<String>,
    pub(crate) offset: Option<i64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IndexedOperationError {
    pub(crate) history_sequence: u64,
    pub(crate) code: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IndexedProducerCancellation {
    pub(crate) history_sequence: u64,
    pub(crate) outcomes: Vec<testlab_schema::ProducerCancellationOutcome>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IndexedClientMetrics {
    pub(crate) history_sequence: u64,
    pub(crate) observation: testlab_schema::ClientMetricsObservation,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IndexedAssignedConsumerControl {
    pub(crate) history_sequence: u64,
    pub(crate) completion: testlab_schema::AssignedConsumerControlCompletion,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IndexedGroupConsumerControl {
    pub(crate) history_sequence: u64,
    pub(crate) completion: testlab_schema::GroupConsumerControlCompletion,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IndexedCommandFailure {
    pub(crate) history_sequence: u64,
    pub(crate) command_id: CommandId,
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
pub(crate) struct IndexedGroupAssignments {
    pub(crate) history_sequence: u64,
    pub(crate) observation: GroupAssignmentsObservation,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IndexedGroupReceiveSet {
    pub(crate) history_sequence: u64,
    pub(crate) completion: GroupReceiveSetCompletion,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IndexedTransactionCompletion {
    pub(crate) history_sequence: u64,
    pub(crate) disposition: TransactionDisposition,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IndexedTransactionalTransform {
    pub(crate) history_sequence: u64,
    pub(crate) completion: TransactionalTransformCompletion,
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
    pub(crate) acquisition_count: usize,
    pub(crate) member_epoch: Option<i32>,
    pub(crate) assignment_epoch: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IndexedShareAcknowledgement {
    pub(crate) history_sequence: u64,
    pub(crate) receive_id: OperationId,
    pub(crate) dispositions: Vec<ShareDisposition>,
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
    command_sequences: Vec<u64>,
    pub(crate) commands: Vec<(u64, CommandId, testlab_schema::AdapterCommand)>,
    pub(crate) adapter_events: Vec<(u64, testlab_schema::AdapterEventEnvelope)>,
    clients_create_issued: BTreeSet<ClientId>,
    clients_ready_issued: BTreeSet<ClientId>,
    client_metrics_issued: BTreeMap<OperationId, ClientId>,
    producers_create_issued: BTreeSet<ProducerId>,
    consumers_create_issued: BTreeSet<ConsumerId>,
    assignments_issued: BTreeSet<ConsumerId>,
    pub(crate) assigned_controls_issued:
        BTreeMap<OperationId, testlab_schema::AssignedConsumerControlCommand>,
    receives_issued: BTreeSet<OperationId>,
    group_assignments_issued: BTreeSet<OperationId>,
    group_receive_sets_issued: BTreeSet<OperationId>,
    pub(crate) group_controls_issued:
        BTreeMap<OperationId, testlab_schema::GroupConsumerControlCommand>,
    group_shutdowns_issued: BTreeSet<OperationId>,
    consumers_close_issued: BTreeSet<ConsumerId>,
    group_consumers_create_issued: BTreeSet<ConsumerId>,
    group_consumers_close_issued: BTreeSet<ConsumerId>,
    share_consumers_create_issued: BTreeSet<ConsumerId>,
    share_receives_issued: BTreeSet<OperationId>,
    share_acknowledgements_issued: BTreeSet<OperationId>,
    share_batches_drop_issued: BTreeSet<OperationId>,
    share_consumers_close_issued: BTreeSet<ConsumerId>,
    operations_issued: BTreeSet<OperationId>,
    admin_commands: Vec<(u64, CommandId, testlab_schema::AdapterCommand)>,
    transactional_producers_create_issued: BTreeSet<ProducerId>,
    transactions_execute_issued: BTreeSet<OperationId>,
    transactional_producers_close_issued: BTreeSet<ProducerId>,
    flushes_issued: BTreeSet<ProducerId>,
    producers_close_issued: BTreeSet<ProducerId>,
    clients_shutdown_issued: BTreeSet<ClientId>,
    finish_issued: bool,
    pub(crate) concurrent_starts:
        BTreeMap<testlab_schema::ConcurrencyId, Vec<IndexedConcurrentStart>>,
    pub(crate) concurrent_joins:
        BTreeMap<testlab_schema::ConcurrencyId, Vec<IndexedConcurrentJoin>>,
    pub(crate) concurrent_started:
        BTreeMap<testlab_schema::ConcurrencyId, Vec<IndexedConcurrentBoundary>>,
    pub(crate) concurrent_actor_completions:
        BTreeMap<testlab_schema::ConcurrencyId, Vec<IndexedConcurrentActorCompletion>>,
    pub(crate) concurrent_completed:
        BTreeMap<testlab_schema::ConcurrencyId, Vec<IndexedConcurrentBoundary>>,
    pub(crate) concurrent_public_events: Vec<IndexedConcurrentPublicEvent>,
    pub(crate) ready: Vec<(u64, AdapterDescriptor)>,
    pub(crate) accepted: BTreeMap<OperationId, Vec<u64>>,
    pub(crate) rejected: BTreeMap<OperationId, Vec<u64>>,
    pub(crate) terminals: BTreeMap<OperationId, Vec<IndexedTerminal>>,
    pub(crate) producer_cancellations: BTreeMap<OperationId, Vec<IndexedProducerCancellation>>,
    pub(crate) operation_errors: BTreeMap<OperationId, Vec<IndexedOperationError>>,
    pub(crate) topics_created: BTreeMap<OperationId, Vec<IndexedAdminTopicCompletion>>,
    pub(crate) topics_creation_completed:
        BTreeMap<OperationId, Vec<IndexedAdminTopicsCreationBatch>>,
    pub(crate) topic_partitions_created: BTreeMap<OperationId, Vec<IndexedAdminTopicCompletion>>,
    pub(crate) topics_deleted: BTreeMap<OperationId, Vec<IndexedAdminTopicCompletion>>,
    pub(crate) topics_described: BTreeMap<OperationId, Vec<IndexedTopicDescription>>,
    pub(crate) topics_listed: BTreeMap<OperationId, Vec<IndexedTopicsList>>,
    pub(crate) offsets_listed: BTreeMap<OperationId, Vec<IndexedOffsetList>>,
    pub(crate) records_deleted: BTreeMap<OperationId, Vec<IndexedRecordsDeleted>>,
    pub(crate) topic_configs_described: BTreeMap<OperationId, Vec<IndexedTopicConfigDescription>>,
    pub(crate) topic_configs_altered: BTreeMap<OperationId, Vec<IndexedAdminTopicConfigCompletion>>,
    pub(crate) admin_validations: admin_validation::AdminValidationIndex,
    pub(crate) admin_group_batches: admin_group_batch::AdminGroupBatchIndex,
    pub(crate) clusters_described: BTreeMap<OperationId, Vec<IndexedClusterDescription>>,
    pub(crate) consumer_groups_listed: BTreeMap<OperationId, Vec<IndexedConsumerGroupsList>>,
    pub(crate) consumer_groups_described:
        BTreeMap<OperationId, Vec<IndexedConsumerGroupDescription>>,
    pub(crate) consumer_group_offsets_listed:
        BTreeMap<OperationId, Vec<IndexedConsumerGroupOffset>>,
    pub(crate) consumer_group_offsets_altered:
        BTreeMap<OperationId, Vec<IndexedAdminGroupOffsetCompletion>>,
    pub(crate) consumer_group_offsets_deleted:
        BTreeMap<OperationId, Vec<IndexedAdminGroupOffsetCompletion>>,
    pub(crate) consumer_groups_deleted: BTreeMap<OperationId, Vec<IndexedAdminGroupCompletion>>,
    pub(crate) topics_observed: BTreeMap<OperationId, Vec<IndexedTopicObservation>>,
    pub(crate) clusters_observed: BTreeMap<OperationId, Vec<IndexedClusterObservation>>,
    pub(crate) consumer_groups_observed:
        BTreeMap<OperationId, Vec<IndexedConsumerGroupObservation>>,
    pub(crate) consumer_group_offsets_observed:
        BTreeMap<OperationId, Vec<IndexedConsumerGroupOffsetObservation>>,
    pub(crate) topic_configs_observed: BTreeMap<OperationId, Vec<IndexedTopicConfigObservation>>,
    pub(crate) partition_offsets_observed:
        BTreeMap<OperationId, Vec<IndexedPartitionOffsetsObservation>>,
    pub(crate) transactional_producers_created: BTreeMap<ProducerId, Vec<u64>>,
    pub(crate) transactions_completed: BTreeMap<OperationId, Vec<IndexedTransactionCompletion>>,
    pub(crate) transactional_transforms: BTreeMap<OperationId, Vec<IndexedTransactionalTransform>>,
    pub(crate) transactions_fenced: BTreeMap<OperationId, Vec<IndexedTransactionFence>>,
    pub(crate) transactional_producers_closed: BTreeMap<ProducerId, Vec<u64>>,
    pub(crate) clients_created: BTreeMap<ClientId, Vec<u64>>,
    pub(crate) clients_ready: BTreeMap<ClientId, Vec<u64>>,
    pub(crate) client_metrics: BTreeMap<OperationId, Vec<IndexedClientMetrics>>,
    pub(crate) producers_created: BTreeMap<ProducerId, Vec<u64>>,
    pub(crate) consumers_created: BTreeMap<ConsumerId, Vec<u64>>,
    pub(crate) assignments: BTreeMap<ConsumerId, Vec<u64>>,
    pub(crate) assigned_controls: BTreeMap<OperationId, Vec<IndexedAssignedConsumerControl>>,
    pub(crate) receives: BTreeMap<OperationId, Vec<IndexedReceive>>,
    pub(crate) group_assignments: BTreeMap<OperationId, Vec<IndexedGroupAssignments>>,
    pub(crate) group_receive_sets: BTreeMap<OperationId, Vec<IndexedGroupReceiveSet>>,
    pub(crate) group_controls: BTreeMap<OperationId, Vec<IndexedGroupConsumerControl>>,
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
    pub(crate) adversary_controls: Vec<(u64, testlab_schema::ProtocolFaultAction)>,
    pub(crate) adversary_observations: Vec<(u64, testlab_schema::ProtocolAdversaryObservation)>,
    pub(crate) network_proxy_controls: Vec<(u64, testlab_schema::NetworkProxyControl)>,
    pub(crate) network_proxy_observations: Vec<(u64, testlab_schema::NetworkProxyObservation)>,
    pub(crate) environment_operations: Vec<(u64, EnvironmentOperation)>,
}

impl HistoryIndex {
    pub(crate) fn build(history: &[HistoryEntry]) -> Self {
        let mut index = Self::default();
        for entry in history {
            index.record(entry);
        }
        index
    }

    pub(crate) fn finish_issued(&self) -> bool {
        !self.has_harness_commands || self.finish_issued
    }
}

fn push<K: Ord>(map: &mut BTreeMap<K, Vec<u64>>, key: K, sequence: u64) {
    map.entry(key).or_default().push(sequence);
}
