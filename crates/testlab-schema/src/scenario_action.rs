//! Versioned scenario action vocabulary shared by manifests and validation.

use serde::{Deserialize, Serialize};

use crate::{
    AdminOffsetPosition, BatchRecord, BrokerBehavior, ClientId, ConsumerId, GroupProtocol,
    OperationId, ProducerId, RecordSpec, TransactionDisposition,
};

/// Scenario action vocabulary for scenario schema v12.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ScenarioAction {
    /// Creates one public client.
    CreateClient {
        /// New client identity.
        client_id: ClientId,
    },
    /// Waits for one explicit public client readiness probe.
    AwaitClientReady {
        /// Existing live client.
        client_id: ClientId,
    },
    /// Creates one public producer.
    CreateProducer {
        /// Existing owning client.
        client_id: ClientId,
        /// New producer identity.
        producer_id: ProducerId,
    },
    /// Selects the next self-test broker outcome.
    SetBrokerBehavior {
        /// One-shot broker behavior.
        behavior: BrokerBehavior,
    },
    /// Restarts one environment-owned broker and waits for Kafka readiness.
    RestartBroker {
        /// One-based ordinal in the environment's declared broker service order.
        broker_ordinal: u16,
        /// Complete restart and readiness bound.
        timeout_ms: u64,
    },
    /// Offers one record.
    Send {
        /// Existing open producer.
        producer_id: ProducerId,
        /// New operation identity.
        operation_id: OperationId,
        /// Exact logical record.
        record: RecordSpec,
    },
    /// Offers an ordered record batch through one public call.
    SendBatch {
        /// Existing open producer.
        producer_id: ProducerId,
        /// Ordered records with stable operation identities.
        operations: Vec<BatchRecord>,
    },
    /// Claims one directly assigned public consumer.
    CreateAssignedConsumer {
        /// Existing owning client.
        client_id: ClientId,
        /// New consumer identity.
        consumer_id: ConsumerId,
    },
    /// Replaces one consumer's assignment at the beginning of one partition.
    AssignBeginning {
        /// Existing open consumer.
        consumer_id: ConsumerId,
        /// Exact topic.
        topic: String,
        /// Exact partition.
        partition: i32,
    },
    /// Bounded receive that must expose one previously sent exact record.
    Receive {
        /// Existing assigned consumer.
        consumer_id: ConsumerId,
        /// Stable identity for the receive operation.
        receive_id: OperationId,
        /// Producer operation whose record must appear exactly once.
        expected_operation_id: OperationId,
        /// Adapter-side public observation bound.
        timeout_ms: u64,
    },
    /// Closes one directly assigned consumer.
    CloseAssignedConsumer {
        /// Consumer to close.
        consumer_id: ConsumerId,
    },
    /// Registers one consumer-group member with an explicit protocol.
    CreateGroupConsumer {
        /// Existing owning client.
        client_id: ClientId,
        /// New consumer identity.
        consumer_id: ConsumerId,
        /// Exact Kafka group identity.
        group_id: String,
        /// Subscribed topic.
        topic: String,
        /// Classic or KIP-848 group protocol.
        protocol: GroupProtocol,
    },
    /// Receives one group batch and commits its assignment-fenced checkpoint.
    GroupReceive {
        /// Existing group consumer.
        consumer_id: ConsumerId,
        /// Stable receive operation identity.
        receive_id: OperationId,
        /// Producer operation whose record must appear exactly once.
        expected_operation_id: OperationId,
        /// Adapter-side public observation bound.
        timeout_ms: u64,
    },
    /// Closes one classic group consumer.
    CloseGroupConsumer {
        /// Consumer to close.
        consumer_id: ConsumerId,
    },
    /// Creates one topic through the packaged client's public admin surface.
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
    /// Increases one Kafka topic to a requested total partition count.
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
        /// Exact partition indices the verifier requires.
        expected_partitions: Vec<i32>,
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
        /// Topics the verifier requires in the public result.
        required_topics: Vec<String>,
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
        /// Exact nonnegative offset the verifier requires.
        expected_offset: i64,
        /// Complete public operation bound.
        timeout_ms: u64,
    },
    /// Initializes one uniquely controlled public transactional producer.
    CreateTransactionalProducer {
        /// Existing owning client.
        client_id: ClientId,
        /// New transactional producer handle identity.
        producer_id: ProducerId,
        /// Exact Kafka transactional identity.
        transactional_id: String,
        /// Broker-side transaction timeout.
        transaction_timeout_ms: u64,
        /// Complete public initialization bound.
        initialization_timeout_ms: u64,
    },
    /// Runs one linear transaction through send and commit or abort.
    ExecuteTransaction {
        /// Existing transactional producer.
        producer_id: ProducerId,
        /// Stable transaction operation identity.
        transaction_id: OperationId,
        /// Ordered transactional records.
        operations: Vec<BatchRecord>,
        /// Requested public transaction outcome.
        disposition: TransactionDisposition,
        /// Complete begin, send, and end bound.
        timeout_ms: u64,
    },
    /// Stages one record, initializes a replacement owner, and observes the old commit result.
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
    /// Flushes one open producer.
    Flush {
        /// Producer to flush.
        producer_id: ProducerId,
    },
    /// Closes one open producer.
    CloseProducer {
        /// Producer to close.
        producer_id: ProducerId,
    },
    /// Shuts down one client after its producers close.
    ShutdownClient {
        /// Client to shut down.
        client_id: ClientId,
    },
}
