#![allow(missing_docs, reason = "typed payload variants are self-describing")]
use crate::{BatchRecord, ClientId, ConsumerId, OperationId, ProducerId};
#[derive(Clone, Debug, serde::Deserialize, Eq, PartialEq, serde::Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AdapterCommand {
    Hello {
        /// Unique test attempt.
        run_id: crate::RunId,
        /// Stable scenario identity.
        scenario_id: crate::ScenarioId,
        /// Ordered broker or test-peer bootstrap endpoints selected by testctl.
        broker_endpoints: Vec<String>,
        /// Non-secret connection policy and secret environment references.
        security: crate::AdapterSecurity,
    },
    CreateClient(crate::CreateClientCommand),
    CreateConfiguredClient(crate::CreateConfiguredClientAction),
    CreateAssignedConsumerClient(crate::CreateAssignedConsumerClientAction),
    /// Waits for one public client readiness probe.
    AwaitClientReady {
        client_id: ClientId,
    },
    ObserveClientMetrics(crate::ObserveClientMetricsCommand),
    /// Creates one public producer handle.
    CreateProducer {
        client_id: ClientId,
        producer_id: ProducerId,
        ownership: crate::ChildHandleOwnership,
    },
    /// Offers one record through the public producer surface.
    Send {
        producer_id: ProducerId,
        operation_id: OperationId,
        /// Exact public partition-selection path.
        partitioning: crate::ProducerPartitioning,
        /// Exact logical record.
        record: crate::RecordSpec,
    },
    /// Accepts one send and requests cancellation twice without losing its observer.
    CancelProducerSend(crate::CancelProducerSendCommand),
    /// Offers an ordered record batch through one public producer call.
    SendBatch {
        /// Producer receiving the records.
        producer_id: ProducerId,
        /// Ordered records with stable operation identities.
        operations: Vec<BatchRecord>,
    },
    /// Releases one caller-ordered public actor set through a shared start barrier.
    StartConcurrentActors(crate::StartConcurrentActorsCommand),
    /// Joins every actor released by one prior concurrent start.
    JoinConcurrentActors {
        /// Stable concurrent group identity.
        concurrency_id: crate::ConcurrencyId,
        /// Complete join bound.
        timeout_ms: u64,
    },
    CreateAssignedConsumer {
        client_id: ClientId,
        consumer_id: ConsumerId,
        ownership: crate::ChildHandleOwnership,
    },
    /// Assigns one consumer at the beginning of one partition.
    AssignBeginning {
        /// Existing consumer.
        consumer_id: ConsumerId,
        /// Exact topic.
        topic: String,
        /// Exact partition.
        partition: i32,
    },
    /// Assigns multiple partitions at their beginnings through one public call.
    AssignBeginningBatch(crate::AssignBeginningBatchCommand),
    ControlAssignedConsumer(crate::AssignedConsumerControlCommand),
    /// Observes public consumer batches for a bounded duration.
    Receive {
        /// Existing assigned consumer.
        consumer_id: ConsumerId,
        /// Stable receive operation identity.
        receive_id: OperationId,
        /// Maximum public observation duration.
        timeout_ms: u64,
    },
    /// Closes one directly assigned consumer.
    CloseAssignedConsumer {
        /// Consumer to close.
        consumer_id: ConsumerId,
    },
    /// Registers one consumer-group member with an explicit protocol.
    CreateGroupConsumer {
        client_id: ClientId,
        consumer_id: ConsumerId,
        group_id: String,
        /// Caller-ordered distinct subscribed topics.
        topics: Vec<String>,
        /// Classic or KIP-848 group protocol.
        protocol: crate::GroupProtocol,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        configuration: Option<crate::GroupConsumerConfiguration>,
    },
    /// Receives one group batch and commits its checkpoint.
    GroupReceive {
        /// Existing group consumer.
        consumer_id: ConsumerId,
        /// Stable receive operation identity.
        receive_id: OperationId,
        /// Maximum public observation duration.
        timeout_ms: u64,
    },
    /// Observes stable public assignments across declared group consumers.
    ObserveGroupAssignments(crate::ObserveGroupAssignmentsCommand),
    /// Receives and commits a structural record count across declared group consumers.
    GroupReceiveSet(crate::GroupReceiveSetCommand),
    ControlGroupConsumer(crate::GroupConsumerControlCommand),
    ShutdownGroupConsumer(crate::GroupConsumerShutdownCommand),
    CloseGroupConsumer {
        consumer_id: ConsumerId,
    },
    AbandonGroupConsumer(crate::GroupConsumerAbandonment),
    /// Registers one unique share-group member.
    CreateShareConsumer {
        client_id: ClientId,
        consumer_id: ConsumerId,
        group_id: String,
        /// Caller-ordered distinct subscribed topics.
        topics: Vec<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        rack: Option<String>,
        /// Complete first-heartbeat bound.
        membership_timeout_ms: u64,
        /// Complete graceful-close bound.
        close_timeout_ms: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        configuration: Option<crate::ShareConsumerFetchConfiguration>,
    },
    ShareReceive {
        /// Existing share consumer.
        consumer_id: ConsumerId,
        /// Stable retained-batch identity.
        receive_id: OperationId,
        /// Complete public observation bound.
        timeout_ms: u64,
    },
    /// Consumes one retained share batch into record-ordered dispositions.
    ShareAcknowledge {
        /// Existing share consumer that owns the session.
        consumer_id: ConsumerId,
        /// Retained batch consumed by this acknowledgement.
        receive_id: OperationId,
        /// Stable acknowledgement identity.
        acknowledgement_id: OperationId,
        /// One public disposition per retained record.
        dispositions: Vec<crate::ShareDisposition>,
        /// Complete acknowledgement bound.
        timeout_ms: u64,
    },
    /// Drops one retained batch without sending an acknowledgement.
    DropShareBatch {
        /// Share consumer that produced the batch.
        consumer_id: ConsumerId,
        /// Retained batch abandoned without acknowledgement.
        receive_id: OperationId,
    },
    /// Closes one unique share-group member.
    CloseShareConsumer {
        /// Unique share consumer consumed by close.
        consumer_id: ConsumerId,
    },
    CreateTopic(crate::CreateTopicCommand),
    CreateTopicsBatch(crate::CreateTopicsBatchCommand),
    CreatePartitions(crate::CreatePartitionsCommand),
    DeleteTopic(crate::DeleteTopicCommand),
    DeleteTopics(crate::DeleteTopicsCommand),
    /// Describes one Kafka topic through the public admin surface.
    DescribeTopic(crate::DescribeTopicCommand),
    DescribeTopics(crate::DescribeTopicsCommand),
    ListTopics(crate::ListTopicsCommand),
    ListConfigResources(crate::ListConfigResourcesCommand),
    ListOffsets(crate::ListOffsetsCommand),
    ListOffsetsBatch(crate::ListOffsetsBatchCommand),
    DeleteRecords(crate::DeleteRecordsCommand),
    DeleteRecordsBatch(crate::DeleteRecordsBatchCommand),
    DescribeTopicConfig(crate::DescribeTopicConfigCommand),
    DescribeTopicConfigs(crate::DescribeTopicConfigsCommand),
    AlterTopicConfigs(crate::AlterTopicConfigsCommand),
    AlterTopicConfig(crate::AlterTopicConfigCommand),
    DescribeCluster(crate::DescribeClusterCommand),
    UnregisterBroker(crate::UnregisterBrokerCommand),
    DescribeFeatures(crate::DescribeFeaturesCommand),
    ValidateFeatureUpdates(crate::ValidateFeatureUpdatesCommand),
    ExerciseDelegationTokenLifecycle(crate::ExerciseDelegationTokenLifecycleCommand),
    ExerciseStreamsGroupAdminLifecycle(crate::ExerciseStreamsGroupAdminLifecycleCommand),
    DescribeProducers(crate::DescribeProducersCommand),
    DescribeLogDirs(crate::DescribeLogDirsCommand),
    DescribeReplicaLogDirs(crate::DescribeReplicaLogDirsCommand),
    AlterReplicaLogDirs(crate::AlterReplicaLogDirsCommand),
    DescribeMetadataQuorum(crate::DescribeMetadataQuorumCommand),
    ListTransactions(crate::ListTransactionsCommand),
    DescribeTransactions(crate::DescribeTransactionsCommand),
    FenceProducers(crate::FenceProducersCommand),
    AlterPartitionReassignments(crate::AlterPartitionReassignmentsCommand),
    ListPartitionReassignments(crate::ListPartitionReassignmentsCommand),
    ElectLeaders(crate::ElectLeadersCommand),
    ListConsumerGroups(crate::ListConsumerGroupsCommand),
    DescribeConsumerGroup(crate::DescribeConsumerGroupCommand),
    DescribeConsumerGroups(crate::DescribeConsumerGroupsCommand),
    DescribeShareGroup(crate::DescribeShareGroupCommand),
    DescribeShareGroups(crate::DescribeShareGroupsCommand),
    ListShareGroupOffsets(crate::ListShareGroupOffsetsCommand),
    ListShareGroupsOffsets(crate::ListShareGroupsOffsetsCommand),
    AlterShareGroupOffsets(crate::AlterShareGroupOffsetsCommand),
    DeleteShareGroupOffsets(crate::DeleteShareGroupOffsetsCommand),
    DeleteShareGroups(crate::DeleteShareGroupsCommand),
    /// Lists one committed consumer-group offset through the public admin surface.
    ListConsumerGroupOffsets(crate::ListConsumerGroupOffsetsCommand),
    /// Lists selected offsets from one consumer group through one public call.
    ListConsumerGroupOffsetsBatch(crate::ListConsumerGroupOffsetsBatchCommand),
    /// Lists selected offsets from multiple consumer groups through one public call.
    ListConsumerGroupsOffsets(crate::ListConsumerGroupsOffsetsCommand),
    /// Alters one committed consumer-group offset through the public admin surface.
    AlterConsumerGroupOffset(crate::AlterConsumerGroupOffsetCommand),
    /// Alters multiple committed offsets through one public admin call.
    AlterConsumerGroupOffsets(crate::AlterConsumerGroupOffsetsCommand),
    /// Deletes one committed consumer-group offset through the public admin surface.
    DeleteConsumerGroupOffset(crate::DeleteConsumerGroupOffsetCommand),
    DeleteConsumerGroupOffsets(crate::DeleteConsumerGroupOffsetsCommand),
    DeleteConsumerGroup(crate::DeleteConsumerGroupCommand),
    DeleteConsumerGroups(crate::DeleteConsumerGroupsCommand),
    RemoveConsumerGroupMembers(crate::RemoveConsumerGroupMembersCommand),
    DescribeClassicGroups(crate::DescribeClassicGroupsCommand),
    CreateAcls(crate::CreateAclsCommand),
    DescribeAcls(crate::DescribeAclsCommand),
    DeleteAcls(crate::DeleteAclsCommand),
    AlterClientQuota(crate::AlterClientQuotaCommand),
    DescribeClientQuota(crate::DescribeClientQuotaCommand),
    AlterUserScramCredential(crate::AlterUserScramCredentialCommand),
    DescribeUserScramCredential(crate::DescribeUserScramCredentialCommand),
    CreateTransactionalProducer {
        /// Owning client.
        client_id: ClientId,
        producer_id: ProducerId,
        /// Exact Kafka transactional identity.
        transactional_id: String,
        transaction_timeout_ms: u64,
        initialization_timeout_ms: u64,
    },
    /// Runs one bounded linear public transaction.
    ExecuteTransaction {
        /// Existing transactional producer.
        producer_id: ProducerId,
        /// Stable transaction operation identity.
        transaction_id: OperationId,
        /// Ordered records staged by the transaction.
        operations: Vec<BatchRecord>,
        /// Requested public transaction terminal operation.
        disposition: crate::TransactionDisposition,
        /// Complete begin, send, and end bound.
        timeout_ms: u64,
    },
    /// Atomically transforms one public group batch and its checkpoint.
    ExecuteTransactionalTransform(crate::TransactionalTransformCommand),
    /// Stages one record, applies one public fence, and proves replacement use.
    FenceTransaction {
        /// Public operation that fences the active transaction.
        fence_method: crate::TransactionFenceMethod,
        /// Existing transactional producer whose active transaction is fenced.
        producer_id: ProducerId,
        /// Stable fenced transaction identity.
        transaction_id: OperationId,
        /// Exact record staged before replacement initialization.
        operation: BatchRecord,
        /// Existing client that owns the replacement producer.
        replacement_client_id: ClientId,
        /// New replacement transactional producer handle.
        replacement_producer_id: ProducerId,
        /// Kafka transactional identity shared with the original producer.
        transactional_id: String,
        /// Broker-side timeout for the replacement producer.
        transaction_timeout_ms: u64,
        /// Complete replacement initialization bound.
        initialization_timeout_ms: u64,
        /// Complete stage, replacement, and old-commit bound.
        timeout_ms: u64,
    },
    CloseTransactionalProducer {
        producer_id: ProducerId,
    },
    Flush {
        producer_id: ProducerId,
    },
    CloseProducer {
        producer_id: ProducerId,
    },
    ShutdownClient {
        client_id: ClientId,
    },
    Finish,
    Abort,
}
