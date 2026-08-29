//! Adapter commands are the public operations testctl may request.
#![allow(missing_docs, reason = "typed payload variants are self-describing")]

use crate::{BatchRecord, ClientId, ConsumerId, OperationId, ProducerId};

/// Public operation requested from an adapter.
#[derive(Clone, Debug, serde::Deserialize, Eq, PartialEq, serde::Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AdapterCommand {
    /// Starts one adapter session and declares the environment endpoints.
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
    /// Creates one public client handle.
    CreateClient {
        /// Scenario-local client identity.
        client_id: ClientId,
    },
    CreateConfiguredClient(crate::CreateConfiguredClientAction),
    /// Waits for one public client readiness probe.
    AwaitClientReady {
        /// Existing client identity.
        client_id: ClientId,
    },
    /// Observes one bounded public client metrics snapshot.
    ObserveClientMetrics(crate::ObserveClientMetricsCommand),
    /// Creates one public producer handle.
    CreateProducer {
        /// Owning client.
        client_id: ClientId,
        /// Scenario-local producer identity.
        producer_id: ProducerId,
    },
    /// Offers one record through the public producer surface.
    Send {
        /// Producer receiving the record.
        producer_id: ProducerId,
        /// Stable operation identity.
        operation_id: OperationId,
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
    /// Claims one directly assigned consumer handle.
    CreateAssignedConsumer {
        /// Owning client.
        client_id: ClientId,
        /// Scenario-local consumer identity.
        consumer_id: ConsumerId,
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
    /// Applies one operation-identified direct-consumer control.
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
        /// Subscribed topic.
        topic: String,
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
        /// Consumer to close.
        consumer_id: ConsumerId,
    },
    /// Registers one unique share-group member.
    CreateShareConsumer {
        /// Existing client that owns the member.
        client_id: ClientId,
        /// New scenario-local share-consumer identity.
        consumer_id: ConsumerId,
        /// Exact Kafka share-group identity.
        group_id: String,
        /// Sole subscribed topic.
        topic: String,
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
    /// Creates one Kafka topic through the public admin surface.
    CreateTopic(crate::CreateTopicCommand),
    /// Creates an ordered Kafka topic batch through one public admin call.
    CreateTopicsBatch(crate::CreateTopicsBatchCommand),
    /// Increases one Kafka topic through the public admin surface.
    CreatePartitions(crate::CreatePartitionsCommand),
    /// Deletes one Kafka topic through the public admin surface.
    DeleteTopic(crate::DeleteTopicCommand),
    /// Describes one Kafka topic through the public admin surface.
    DescribeTopic(crate::DescribeTopicCommand),
    /// Lists Kafka topics visible through the public admin surface.
    ListTopics(crate::ListTopicsCommand),
    /// Lists one offset position through the public admin surface.
    ListOffsets(crate::ListOffsetsCommand),
    /// Deletes records before one exact partition offset.
    DeleteRecords(crate::DeleteRecordsCommand),
    /// Describes one selected topic configuration through the public admin surface.
    DescribeTopicConfig(crate::DescribeTopicConfigCommand),
    /// Replaces one selected topic configuration through the public admin surface.
    AlterTopicConfig(crate::AlterTopicConfigCommand),
    /// Describes the connected Kafka cluster through the public admin surface.
    DescribeCluster(crate::DescribeClusterCommand),
    /// Lists consumer groups visible through the public admin surface.
    ListConsumerGroups(crate::ListConsumerGroupsCommand),
    /// Describes one consumer group through the public admin surface.
    DescribeConsumerGroup(crate::DescribeConsumerGroupCommand),
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
    /// Deletes multiple committed offsets through one public admin call.
    DeleteConsumerGroupOffsets(crate::DeleteConsumerGroupOffsetsCommand),
    /// Deletes one consumer group through the public admin surface.
    DeleteConsumerGroup(crate::DeleteConsumerGroupCommand),
    /// Describes multiple classic consumer groups through one public admin call.
    DescribeClassicGroups(crate::DescribeClassicGroupsCommand),
    /// Initializes one public transactional producer.
    CreateTransactionalProducer {
        /// Owning client.
        client_id: ClientId,
        /// Scenario-local transactional producer identity.
        producer_id: ProducerId,
        /// Exact Kafka transactional identity.
        transactional_id: String,
        /// Broker-side transaction timeout.
        transaction_timeout_ms: u64,
        /// Complete public initialization bound.
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
        /// Requested public transaction outcome.
        disposition: crate::TransactionDisposition,
        /// Complete begin, send, and end bound.
        timeout_ms: u64,
    },
    /// Atomically transforms one public group batch and its checkpoint.
    ExecuteTransactionalTransform(crate::TransactionalTransformCommand),
    /// Stages one record and initializes a replacement owner before the old commit.
    FenceTransaction {
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
    /// Closes one idle transactional producer.
    CloseTransactionalProducer {
        /// Transactional producer to close.
        producer_id: ProducerId,
    },
    /// Flushes one producer.
    Flush {
        /// Producer to flush.
        producer_id: ProducerId,
    },
    /// Closes one producer.
    CloseProducer {
        /// Producer to close.
        producer_id: ProducerId,
    },
    /// Shuts down one client.
    ShutdownClient {
        /// Client to shut down.
        client_id: ClientId,
    },
    /// Ends the adapter session after lifecycle work settles.
    Finish,
    /// Abandons the adapter session after testctl has observed a scenario failure.
    Abort,
}
