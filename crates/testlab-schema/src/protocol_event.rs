use crate::{
    AdapterDescriptor, ClientId, ConsumedRecord, ConsumerId, GroupMembershipEpoch, OperationId,
    ProducerId, ProducerReceipt, ShareConsumedRecord, ShareDisposition, TerminalStatus,
    TransactionDisposition,
};
use serde::{Deserialize, Serialize};
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AdapterEvent {
    Ready {
        /// Adapter identity and capabilities.
        descriptor: AdapterDescriptor,
    },
    /// Public client construction completed with exact returned configuration.
    ClientCreated(crate::ClientConfigurationObservation),
    /// Public client readiness completed.
    ClientReady {
        client_id: ClientId,
    },
    ClientMetricsObserved(Box<crate::ClientMetricsObservation>),
    /// Public producer construction completed with exact selected builder policy.
    ProducerCreated(crate::ProducerHandleConfigurationObservation),
    /// The public producer accepted ownership of one operation.
    OperationAccepted {
        /// Accepted operation.
        operation_id: OperationId,
    },
    /// The public producer rejected admission and retained no ownership.
    OperationRejected {
        /// Rejected operation.
        operation_id: OperationId,
        code: String,
    },
    /// One accepted operation reached its only terminal outcome.
    OperationTerminal {
        operation_id: OperationId,
        status: TerminalStatus,
        code: Option<String>,
        partition: Option<i32>,
        /// Broker offset when exposed by the public surface.
        offset: Option<i64>,
        /// Broker record timestamp when exposed by the public surface.
        timestamp_millis: Option<i64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        receipt: Option<ProducerReceipt>,
    },
    /// Two public cancellation requests completed on one retained observer.
    ProducerCancellationCompleted(crate::ProducerCancellationCompletion),
    /// One public batch call emitted every per-operation outcome.
    BatchCompleted {
        /// Producer that handled the batch.
        producer_id: ProducerId,
    },
    /// Every declared actor crossed one shared public start boundary.
    ConcurrentActorsStarted {
        /// Stable concurrent group identity.
        concurrency_id: crate::ConcurrencyId,
        /// Exact caller-ordered actor identities.
        actor_ids: Vec<crate::ActorId>,
    },
    /// One concurrent actor exposed its complete normal public outcome.
    ConcurrentActorCompleted {
        /// Stable concurrent group identity.
        concurrency_id: crate::ConcurrencyId,
        /// Stable actor identity.
        actor_id: crate::ActorId,
        /// Stable public operation identity owned by the actor.
        operation_id: OperationId,
    },
    /// Every declared concurrent actor was joined in caller order.
    ConcurrentActorsCompleted {
        /// Stable concurrent group identity.
        concurrency_id: crate::ConcurrencyId,
        /// Exact caller-ordered actor identities.
        actor_ids: Vec<crate::ActorId>,
    },
    /// One directly assigned consumer was claimed.
    AssignedConsumerCreated {
        /// Created consumer.
        consumer_id: ConsumerId,
    },
    /// One direct assignment replacement completed.
    AssignmentCompleted {
        /// Assigned consumer.
        consumer_id: ConsumerId,
    },
    /// One operation-identified direct-consumer control completed.
    AssignedConsumerControlCompleted(crate::AssignedConsumerControlCompletion),
    AssignedConsumerEventObserved(crate::AssignedConsumerEventObservation),
    ReceiveCompleted {
        receive_id: OperationId,
        records: Vec<ConsumedRecord>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        fetch_evidence: Option<crate::AssignedConsumerFetchEvidence>,
    },
    AssignedRecordTransferCompleted(crate::AssignedRecordTransferCompletion),
    /// One directly assigned consumer closed.
    AssignedConsumerClosed {
        /// Closed consumer.
        consumer_id: ConsumerId,
    },
    /// One consumer-group member registered with exact returned configuration.
    GroupConsumerCreated(crate::GroupConsumerRegistrationObservation),
    /// One group batch observation and checkpoint attempt completed.
    GroupReceiveCompleted {
        /// Stable receive operation identity.
        receive_id: OperationId,
        /// Exact records returned through the public API.
        records: Vec<ConsumedRecord>,
        /// Whether the assignment-fenced checkpoint committed.
        committed: bool,
        /// Public group metadata observed after the commit.
        group_epoch: Option<GroupMembershipEpoch>,
    },
    /// Stable public assignment snapshots and transitions were observed.
    GroupAssignmentsObserved(crate::GroupAssignmentsObservation),
    /// One multi-member group receive and commit operation completed.
    GroupReceiveSetCompleted(crate::GroupReceiveSetCompletion),
    GroupConsumerControlCompleted(crate::GroupConsumerControlCompletion),
    GroupConsumerShutdownCompleted(crate::GroupConsumerShutdownCompletion),
    GroupConsumerClosed {
        consumer_id: ConsumerId,
    },
    GroupConsumerAbandoned(crate::GroupConsumerAbandonment),
    /// One Share-group member registered with exact returned configuration.
    ShareConsumerCreated(crate::ShareConsumerRegistrationObservation),
    /// One share batch was retained behind its receive identity.
    ShareReceiveCompleted {
        /// Share consumer that retained the batch.
        consumer_id: ConsumerId,
        /// Stable retained-batch identity.
        receive_id: OperationId,
        /// Exact records returned by the public API.
        records: Vec<ShareConsumedRecord>,
        /// Number of broker acquisition ranges retained by the public batch.
        acquisition_count: usize,
        /// Positive broker member epoch when observed.
        member_epoch: Option<i32>,
        /// Positive local assignment fence when observed.
        assignment_epoch: Option<u64>,
    },
    /// One retained share batch reached an acknowledged broker terminal.
    ShareAcknowledgementCompleted {
        /// Stable acknowledgement identity.
        acknowledgement_id: OperationId,
        /// Retained batch consumed by the acknowledgement.
        receive_id: OperationId,
        /// Public dispositions sent in retained record order.
        dispositions: Vec<ShareDisposition>,
        /// Whether every partition reached a successful broker terminal.
        success: bool,
        /// Exact delivery certainty for a failed acknowledgement.
        delivery: Option<TerminalStatus>,
        /// Stable normalized failure code, when failed.
        code: Option<String>,
    },
    /// One retained batch was dropped without network acknowledgement.
    ShareBatchDropped {
        /// Retained batch abandoned without acknowledgement.
        receive_id: OperationId,
    },
    /// One share close reached its public terminal, including uncertainty.
    ShareConsumerClosed {
        /// Unique share consumer consumed by close.
        consumer_id: ConsumerId,
        /// Whether the public close succeeded.
        success: bool,
        /// Exact delivery certainty for a failed close.
        delivery: Option<TerminalStatus>,
        /// Stable normalized failure code, when failed.
        code: Option<String>,
    },
    TopicCreated(crate::AdminTopicCompletion),
    TopicCreationValidated(crate::AdminTopicCompletion),
    TopicsCreationCompleted(crate::AdminTopicsCreationBatch),
    /// One public admin partition-count increase completed successfully.
    TopicPartitionsCreated(crate::AdminTopicCompletion),
    /// One public partition-count increase validated without mutation.
    TopicPartitionIncreaseValidated(crate::AdminTopicCompletion),
    TopicDeleted(crate::AdminTopicCompletion),
    TopicsDeleted(crate::AdminTopicsDeletion),
    TopicDescribed(crate::AdminTopicDescription),
    TopicsDescribed(crate::AdminTopicsDescription),
    TopicsListed(crate::AdminTopicsListing),
    ConfigResourcesListed(crate::AdminConfigResourcesListing),
    OffsetListed(crate::AdminOffsetListing),
    OffsetsListed(crate::AdminOffsetsListing),
    RecordsDeleted(crate::AdminRecordsDeleted),
    RecordsBatchDeleted(crate::AdminRecordsBatchDeleted),
    TopicConfigDescribed(crate::AdminTopicConfigDescription),
    TopicConfigsDescribed(crate::AdminTopicConfigsDescription),
    TopicConfigsAltered(crate::AdminTopicConfigsAlteration),
    TopicConfigAltered(crate::AdminTopicConfigCompletion),
    /// One selected topic-configuration replacement validated without mutation.
    TopicConfigAlterationValidated(crate::AdminTopicConfigCompletion),
    ClusterDescribed(crate::AdminClusterDescription),
    BrokerUnregistered(crate::AdminBrokerUnregistration),
    FeaturesDescribed(crate::AdminFeaturesDescription),
    FeatureUpdatesValidated(crate::AdminFeatureUpdatesValidation),
    DelegationTokenLifecycleExercised(crate::AdminDelegationTokenLifecycle),
    StreamsGroupAdminLifecycleExercised(crate::AdminStreamsGroupAdminLifecycle),
    ProducersDescribed(crate::AdminProducersDescription),
    LogDirsDescribed(crate::AdminLogDirsDescription),
    ReplicaLogDirsDescribed(crate::AdminReplicaLogDirsDescription),
    ReplicaLogDirsAltered(crate::AdminReplicaLogDirsAlteration),
    MetadataQuorumDescribed(crate::AdminMetadataQuorumDescription),
    TransactionsListed(crate::AdminTransactionsListing),
    TransactionsDescribed(crate::AdminTransactionsDescription),
    ProducersFenced(crate::AdminProducersFenced),
    PartitionReassignmentsAltered(crate::AdminPartitionReassignmentsAlteration),
    PartitionReassignmentsListed(crate::AdminPartitionReassignmentsListing),
    LeadersElected(crate::AdminLeaderElection),
    ConsumerGroupsListed(crate::AdminConsumerGroupsListing),
    ConsumerGroupDescribed(crate::AdminConsumerGroupDescription),
    ConsumerGroupsDescribed(crate::AdminConsumerGroupsDescription),
    ShareGroupDescribed(crate::AdminShareGroupDescription),
    ShareGroupsDescribed(crate::AdminShareGroupsDescription),
    ShareGroupOffsetsListed(crate::AdminShareGroupOffsetListing),
    ShareGroupsOffsetsListed(crate::AdminShareGroupsOffsetsListing),
    ShareGroupOffsetsAltered(crate::AdminShareGroupOffsetAlteration),
    ShareGroupOffsetsDeleted(crate::AdminShareGroupOffsetDeletion),
    ShareGroupsDeleted(crate::AdminShareGroupsDeletion),
    ConsumerGroupOffsetListed(crate::AdminConsumerGroupOffsetListing),
    /// One public single-group offset batch listing returned ordered outcomes.
    ConsumerGroupOffsetsListed(crate::AdminConsumerGroupOffsetsListing),
    ConsumerGroupsOffsetsListed(crate::AdminConsumerGroupsOffsetsListing),
    /// One public admin consumer-group offset alteration completed successfully.
    ConsumerGroupOffsetAltered(crate::AdminConsumerGroupOffsetCompletion),
    /// One public plural offset alteration returned ordered outcomes.
    ConsumerGroupOffsetsAltered(crate::AdminConsumerGroupOffsetsMutation),
    /// One public admin consumer-group offset deletion completed successfully.
    ConsumerGroupOffsetDeleted(crate::AdminConsumerGroupOffsetCompletion),
    /// One public plural offset deletion returned ordered outcomes.
    ConsumerGroupOffsetsDeleted(crate::AdminConsumerGroupOffsetsMutation),
    ConsumerGroupDeleted(crate::AdminConsumerGroupCompletion),
    ConsumerGroupsDeleted(crate::AdminConsumerGroupsDeletion),
    ConsumerGroupMembersRemoved(crate::AdminConsumerGroupMembersRemoval),
    ClassicGroupsDescribed(crate::AdminClassicGroupsDescription),
    AclsCreated(crate::AdminAclsCreation),
    AclsDescribed(crate::AdminAclsDescription),
    AclsDeleted(crate::AdminAclsDeletion),
    ClientQuotaAltered(crate::AdminClientQuotaAlteration),
    ClientQuotaAlterationValidated(crate::AdminClientQuotaAlteration),
    ClientQuotaDescribed(crate::AdminClientQuotaDescription),
    UserScramCredentialAltered(crate::AdminUserScramCredentialAlteration),
    UserScramCredentialDescribed(crate::AdminUserScramCredentialDescription),
    /// One public transactional producer initialized with observable owner state.
    TransactionalProducerCreated(crate::TransactionalProducerObservation),
    TransactionCompleted {
        transaction_id: OperationId,
        disposition: TransactionDisposition,
        validated_topic_ids: Vec<[u8; 16]>,
    },
    /// One public transactional transform and checkpoint transfer completed.
    TransactionalTransformCompleted(crate::TransactionalTransformCompletion),
    /// One old transaction exposed its public commit result after replacement initialization.
    TransactionFenceCompleted {
        /// Stable fenced transaction identity.
        transaction_id: OperationId,
        /// Normalized public commit error, or none when the old commit unexpectedly succeeded.
        commit_error_code: Option<String>,
    },
    TransactionalProducerClosed {
        producer_id: ProducerId,
    },
    FlushCompleted {
        producer_id: ProducerId,
    },
    ProducerClosed {
        producer_id: ProducerId,
    },
    ClientShutdown {
        client_id: ClientId,
    },
    /// One public client command returned a normal API failure.
    CommandFailed {
        /// Stable normalized client error code.
        code: String,
        diagnostic: String,
    },
    Finished,
    Aborted,
    /// Adapter cannot continue the session.
    Fatal {
        code: String,
        diagnostic: String,
    },
}
