//! Adapter commands are the public operations testctl may request.

use serde::{Deserialize, Serialize};

use crate::{
    AdapterSecurity, AdminOffsetPosition, BatchRecord, ClientId, ConsumerId, GroupProtocol,
    OperationId, ProducerId, RecordSpec, RunId, ScenarioId, TransactionDisposition,
};

/// Public operation requested from an adapter.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AdapterCommand {
    /// Starts one adapter session and declares the environment endpoints.
    Hello {
        /// Unique test attempt.
        run_id: RunId,
        /// Stable scenario identity.
        scenario_id: ScenarioId,
        /// Ordered broker or test-peer bootstrap endpoints selected by testctl.
        broker_endpoints: Vec<String>,
        /// Non-secret connection policy and secret environment references.
        security: AdapterSecurity,
    },
    /// Creates one public client handle.
    CreateClient {
        /// Scenario-local client identity.
        client_id: ClientId,
    },
    /// Waits for one public client readiness probe.
    AwaitClientReady {
        /// Existing client identity.
        client_id: ClientId,
    },
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
        record: RecordSpec,
    },
    /// Offers an ordered record batch through one public producer call.
    SendBatch {
        /// Producer receiving the records.
        producer_id: ProducerId,
        /// Ordered records with stable operation identities.
        operations: Vec<BatchRecord>,
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
        /// Owning client.
        client_id: ClientId,
        /// Scenario-local consumer identity.
        consumer_id: ConsumerId,
        /// Exact Kafka group identity.
        group_id: String,
        /// Subscribed topic.
        topic: String,
        /// Classic or KIP-848 group protocol.
        protocol: GroupProtocol,
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
    /// Closes one classic group consumer.
    CloseGroupConsumer {
        /// Consumer to close.
        consumer_id: ConsumerId,
    },
    /// Creates one Kafka topic through the public admin surface.
    CreateTopic {
        /// Existing client whose admin handle is used.
        client_id: ClientId,
        /// Stable admin operation identity.
        operation_id: OperationId,
        /// Exact Kafka topic name.
        topic: String,
        /// Positive partition count.
        partitions: i32,
        /// Positive replication factor.
        replication_factor: i16,
        /// Complete public operation bound.
        timeout_ms: u64,
    },
    /// Increases one Kafka topic through the public admin surface.
    CreatePartitions {
        /// Existing client whose admin handle is used.
        client_id: ClientId,
        /// Stable admin operation identity.
        operation_id: OperationId,
        /// Exact Kafka topic name.
        topic: String,
        /// Positive requested total partition count.
        total_count: i32,
        /// Complete public operation bound.
        timeout_ms: u64,
    },
    /// Describes one Kafka topic through the public admin surface.
    DescribeTopic {
        /// Existing client whose admin handle is used.
        client_id: ClientId,
        /// Stable admin operation identity.
        operation_id: OperationId,
        /// Exact Kafka topic name.
        topic: String,
        /// Complete public operation bound.
        timeout_ms: u64,
    },
    /// Lists Kafka topics visible through the public admin surface.
    ListTopics {
        /// Existing client whose admin handle is used.
        client_id: ClientId,
        /// Stable admin operation identity.
        operation_id: OperationId,
        /// Whether broker-marked internal topics enter the public result.
        include_internal: bool,
        /// Complete public operation bound.
        timeout_ms: u64,
    },
    /// Lists one offset position through the public admin surface.
    ListOffsets {
        /// Existing client whose admin handle is used.
        client_id: ClientId,
        /// Stable admin operation identity.
        operation_id: OperationId,
        /// Exact Kafka topic name.
        topic: String,
        /// Exact nonnegative partition.
        partition: i32,
        /// Latest offset position.
        position: AdminOffsetPosition,
        /// Complete public operation bound.
        timeout_ms: u64,
    },
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
        disposition: TransactionDisposition,
        /// Complete begin, send, and end bound.
        timeout_ms: u64,
    },
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
}
