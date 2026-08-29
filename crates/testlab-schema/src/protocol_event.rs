//! Adapter events normalize only facts exposed through a public client surface.
#![allow(missing_docs, reason = "typed payload variants are self-describing")]

use serde::{Deserialize, Serialize};

use crate::{
    AdapterDescriptor, ClientId, ConsumedRecord, ConsumerId, GroupMembershipEpoch, OperationId,
    ProducerId, ShareConsumedRecord, ShareDisposition, TerminalStatus, TransactionDisposition,
};

/// Normalized public event emitted by an adapter.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AdapterEvent {
    /// Successful handshake and capability declaration.
    Ready {
        /// Adapter identity and capabilities.
        descriptor: AdapterDescriptor,
    },
    /// Public client construction completed.
    ClientCreated {
        /// Created client.
        client_id: ClientId,
    },
    /// Public client readiness completed.
    ClientReady {
        /// Ready client.
        client_id: ClientId,
    },
    /// One complete public client metrics snapshot was observed.
    ClientMetricsObserved(Box<crate::ClientMetricsObservation>),
    /// Public producer construction completed.
    ProducerCreated {
        /// Created producer.
        producer_id: ProducerId,
    },
    /// The public producer accepted ownership of one operation.
    OperationAccepted {
        /// Accepted operation.
        operation_id: OperationId,
    },
    /// The public producer rejected admission and retained no ownership.
    OperationRejected {
        /// Rejected operation.
        operation_id: OperationId,
        /// Stable normalized rejection code.
        code: String,
    },
    /// One accepted operation reached its only terminal outcome.
    OperationTerminal {
        /// Settled operation.
        operation_id: OperationId,
        /// Delivery certainty.
        status: TerminalStatus,
        /// Stable normalized outcome code.
        code: Option<String>,
        /// Broker offset when exposed by the public surface.
        offset: Option<i64>,
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
    /// One bounded receive observation completed.
    ReceiveCompleted {
        /// Stable receive operation identity.
        receive_id: OperationId,
        /// Exact records returned through the public API.
        records: Vec<ConsumedRecord>,
    },
    /// One directly assigned consumer closed.
    AssignedConsumerClosed {
        /// Closed consumer.
        consumer_id: ConsumerId,
    },
    /// One consumer-group member registered.
    GroupConsumerCreated {
        /// Created consumer.
        consumer_id: ConsumerId,
    },
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
        /// Closed consumer.
        consumer_id: ConsumerId,
    },
    /// One share-group member registered.
    ShareConsumerCreated {
        /// Created share consumer.
        consumer_id: ConsumerId,
    },
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
    /// One public admin topic creation completed successfully.
    TopicCreated(crate::AdminTopicCompletion),
    /// One public admin topic-creation request validated without mutation.
    TopicCreationValidated(crate::AdminTopicCompletion),
    /// One public admin batch topic-creation call returned ordered outcomes.
    TopicsCreationCompleted(crate::AdminTopicsCreationBatch),
    /// One public admin partition-count increase completed successfully.
    TopicPartitionsCreated(crate::AdminTopicCompletion),
    /// One public partition-count increase validated without mutation.
    TopicPartitionIncreaseValidated(crate::AdminTopicCompletion),
    /// One public admin topic deletion completed successfully.
    TopicDeleted(crate::AdminTopicCompletion),
    /// One public admin topic description completed successfully.
    TopicDescribed(crate::AdminTopicDescription),
    /// One public admin topic listing completed successfully.
    TopicsListed(crate::AdminTopicsListing),
    /// One public admin offset listing completed successfully.
    OffsetListed(crate::AdminOffsetListing),
    /// One public admin prefix deletion completed successfully.
    RecordsDeleted(crate::AdminRecordsDeleted),
    /// One selected public topic configuration was described.
    TopicConfigDescribed(crate::AdminTopicConfigDescription),
    /// One selected public topic configuration was replaced.
    TopicConfigAltered(crate::AdminTopicConfigCompletion),
    /// One selected topic-configuration replacement validated without mutation.
    TopicConfigAlterationValidated(crate::AdminTopicConfigCompletion),
    /// One public admin cluster description completed successfully.
    ClusterDescribed(crate::AdminClusterDescription),
    /// One public admin consumer-group listing completed successfully.
    ConsumerGroupsListed(crate::AdminConsumerGroupsListing),
    /// One public admin consumer-group description completed successfully.
    ConsumerGroupDescribed(crate::AdminConsumerGroupDescription),
    /// One public admin consumer-group offset listing completed successfully.
    ConsumerGroupOffsetListed(crate::AdminConsumerGroupOffsetListing),
    /// One public single-group offset batch listing returned ordered outcomes.
    ConsumerGroupOffsetsListed(crate::AdminConsumerGroupOffsetsListing),
    /// One public multi-group offset listing returned ordered outcomes.
    ConsumerGroupsOffsetsListed(crate::AdminConsumerGroupsOffsetsListing),
    /// One public admin consumer-group offset alteration completed successfully.
    ConsumerGroupOffsetAltered(crate::AdminConsumerGroupOffsetCompletion),
    /// One public plural offset alteration returned ordered outcomes.
    ConsumerGroupOffsetsAltered(crate::AdminConsumerGroupOffsetsMutation),
    /// One public admin consumer-group offset deletion completed successfully.
    ConsumerGroupOffsetDeleted(crate::AdminConsumerGroupOffsetCompletion),
    /// One public plural offset deletion returned ordered outcomes.
    ConsumerGroupOffsetsDeleted(crate::AdminConsumerGroupOffsetsMutation),
    /// One public admin consumer-group deletion completed successfully.
    ConsumerGroupDeleted(crate::AdminConsumerGroupCompletion),
    /// One public classic-group batch description returned ordered outcomes.
    ClassicGroupsDescribed(crate::AdminClassicGroupsDescription),
    /// Public transactional producer initialization completed.
    TransactionalProducerCreated {
        /// Created transactional producer.
        producer_id: ProducerId,
    },
    /// One public transaction reached an observed terminal disposition.
    TransactionCompleted {
        /// Stable transaction operation identity.
        transaction_id: OperationId,
        /// Observed commit or abort outcome.
        disposition: TransactionDisposition,
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
    /// One idle public transactional producer closed.
    TransactionalProducerClosed {
        /// Closed transactional producer.
        producer_id: ProducerId,
    },
    /// Producer flush completed.
    FlushCompleted {
        /// Flushed producer.
        producer_id: ProducerId,
    },
    /// Producer close completed.
    ProducerClosed {
        /// Closed producer.
        producer_id: ProducerId,
    },
    /// Client shutdown completed.
    ClientShutdown {
        /// Shut down client.
        client_id: ClientId,
    },
    /// One public client command returned a normal API failure.
    CommandFailed {
        /// Stable normalized client error code.
        code: String,
        /// Bounded public diagnostic retained as evidence.
        diagnostic: String,
    },
    /// Adapter session settled and may exit.
    Finished,
    /// Adapter session was abandoned after a scenario failure.
    Aborted,
    /// Adapter cannot continue the session.
    Fatal {
        /// Stable adapter failure code.
        code: String,
        /// Bounded diagnostic context.
        diagnostic: String,
    },
}
