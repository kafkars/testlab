//! Scenario actions declare bounded public calls and external broker controls.

use serde::{Deserialize, Serialize};

use crate::{
    BatchRecord, BrokerBehavior, ClientId, ConsumerId, GroupProtocol, OperationId, ProducerId,
    RecordSpec, ShareDisposition, TransactionDisposition,
};
use crate::{CreatePartitionsAction, DescribeTopicAction};
use crate::{ListOffsetsAction, ListTopicsAction};

/// Scenario action vocabulary for scenario schema v13.
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
        /// Existing client identity.
        client_id: ClientId,
    },
    /// Creates one public producer.
    CreateProducer {
        /// Owning client.
        client_id: ClientId,
        /// New producer identity.
        producer_id: ProducerId,
    },
    /// Selects the next self-test broker outcome.
    SetBrokerBehavior {
        /// Next model-broker behavior.
        behavior: BrokerBehavior,
    },
    /// Restarts one environment-owned broker and waits for Kafka readiness.
    RestartBroker {
        /// One-based declared broker ordinal.
        broker_ordinal: u16,
        /// Complete disruption bound.
        timeout_ms: u64,
    },
    /// Stops one declared broker without restoring it in the same action.
    StopBroker {
        /// One-based declared broker ordinal.
        broker_ordinal: u16,
        /// Complete stop bound.
        timeout_ms: u64,
    },
    /// Starts one broker retained by a prior stop action.
    StartBroker {
        /// One-based declared broker ordinal.
        broker_ordinal: u16,
        /// Complete start and readiness bound.
        timeout_ms: u64,
    },
    /// Stops the independently observed leader for one exact partition.
    StopPartitionLeader {
        /// Exact topic.
        topic: String,
        /// Exact partition.
        partition: i32,
        /// Complete election bound.
        timeout_ms: u64,
    },
    /// Restarts the broker previously stopped for one exact partition.
    RestorePartitionLeader {
        /// Exact topic used to identify the stopped broker.
        topic: String,
        /// Exact partition used to identify the stopped broker.
        partition: i32,
        /// Complete restoration bound.
        timeout_ms: u64,
    },
    /// Offers one record.
    Send {
        /// Existing producer.
        producer_id: ProducerId,
        /// Stable operation identity.
        operation_id: OperationId,
        /// Exact record.
        record: RecordSpec,
    },
    /// Offers an ordered record batch through one public call.
    SendBatch {
        /// Existing producer.
        producer_id: ProducerId,
        /// Ordered operations.
        operations: Vec<BatchRecord>,
    },
    /// Claims one directly assigned public consumer.
    CreateAssignedConsumer {
        /// Owning client.
        client_id: ClientId,
        /// New consumer identity.
        consumer_id: ConsumerId,
    },
    /// Replaces one consumer's assignment at the beginning of one partition.
    AssignBeginning {
        /// Existing assigned consumer.
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
        /// Stable receive identity.
        receive_id: OperationId,
        /// Expected producer operation.
        expected_operation_id: OperationId,
        /// Complete receive bound.
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
        /// New consumer identity.
        consumer_id: ConsumerId,
        /// Exact group identity.
        group_id: String,
        /// Subscribed topic.
        topic: String,
        /// Group protocol.
        protocol: GroupProtocol,
    },
    /// Receives one group batch and commits its assignment-fenced checkpoint.
    GroupReceive {
        /// Existing group consumer.
        consumer_id: ConsumerId,
        /// Stable receive identity.
        receive_id: OperationId,
        /// Expected producer operation.
        expected_operation_id: OperationId,
        /// Complete receive and commit bound.
        timeout_ms: u64,
    },
    /// Closes one classic group consumer.
    CloseGroupConsumer {
        /// Consumer to close.
        consumer_id: ConsumerId,
    },
    /// Registers one unique KIP-932 share-group member.
    CreateShareConsumer {
        /// Owning client.
        client_id: ClientId,
        /// New share consumer identity.
        consumer_id: ConsumerId,
        /// Exact share-group identity.
        group_id: String,
        /// Subscribed topic.
        topic: String,
        /// Complete membership-start bound.
        membership_timeout_ms: u64,
        /// Complete close bound.
        close_timeout_ms: u64,
    },
    /// Retains one exact share batch for a later acknowledgement or drop.
    ShareReceive {
        /// Existing share consumer.
        consumer_id: ConsumerId,
        /// Stable retained-batch identity.
        receive_id: OperationId,
        /// Expected producer operation.
        expected_operation_id: OperationId,
        /// Smallest accepted delivery count.
        minimum_delivery_count: i16,
        /// Complete receive bound.
        timeout_ms: u64,
    },
    /// Acknowledges every record in one retained share batch uniformly.
    ShareAcknowledge {
        /// Existing share consumer.
        consumer_id: ConsumerId,
        /// Retained batch identity.
        receive_id: OperationId,
        /// Stable acknowledgement identity.
        acknowledgement_id: OperationId,
        /// Uniform disposition.
        disposition: ShareDisposition,
        /// Complete acknowledgement bound.
        timeout_ms: u64,
    },
    /// Drops one retained share batch without a broker acknowledgement.
    DropShareBatch {
        /// Existing share consumer.
        consumer_id: ConsumerId,
        /// Retained batch identity.
        receive_id: OperationId,
    },
    /// Closes one unique share member and declares whether success is required.
    CloseShareConsumer {
        /// Share consumer to close.
        consumer_id: ConsumerId,
        /// Required public result.
        expect_success: bool,
    },
    /// Creates one topic through the packaged client's public admin surface.
    CreateTopic {
        /// Existing client.
        client_id: ClientId,
        /// Stable admin operation identity.
        operation_id: OperationId,
        /// Topic to create.
        topic: String,
        /// Positive partition count.
        partitions: i32,
        /// Positive replication factor.
        replication_factor: i16,
        /// Complete admin bound.
        timeout_ms: u64,
    },
    /// Increases one Kafka topic to a requested total partition count.
    CreatePartitions(CreatePartitionsAction),
    /// Describes one Kafka topic through the public admin surface.
    DescribeTopic(DescribeTopicAction),
    /// Lists Kafka topics visible through the public admin surface.
    ListTopics(ListTopicsAction),
    /// Lists one offset position through the public admin surface.
    ListOffsets(ListOffsetsAction),
    /// Initializes one uniquely controlled public transactional producer.
    CreateTransactionalProducer {
        /// Owning client.
        client_id: ClientId,
        /// New producer identity.
        producer_id: ProducerId,
        /// Exact transactional identity.
        transactional_id: String,
        /// Broker transaction timeout.
        transaction_timeout_ms: u64,
        /// Complete initialization bound.
        initialization_timeout_ms: u64,
    },
    /// Runs one linear transaction through send and commit or abort.
    ExecuteTransaction {
        /// Existing transactional producer.
        producer_id: ProducerId,
        /// Stable transaction identity.
        transaction_id: OperationId,
        /// Ordered staged operations.
        operations: Vec<BatchRecord>,
        /// Requested transaction outcome.
        disposition: TransactionDisposition,
        /// Complete transaction bound.
        timeout_ms: u64,
    },
    /// Stages one record, initializes a replacement owner, and observes the old commit result.
    FenceTransaction {
        /// Existing transactional producer.
        producer_id: ProducerId,
        /// Stable transaction identity.
        transaction_id: OperationId,
        /// Operation staged before fencing.
        operation: BatchRecord,
        /// Owning client for the replacement.
        replacement_client_id: ClientId,
        /// Replacement producer identity.
        replacement_producer_id: ProducerId,
        /// Shared transactional identity.
        transactional_id: String,
        /// Broker transaction timeout.
        transaction_timeout_ms: u64,
        /// Complete replacement initialization bound.
        initialization_timeout_ms: u64,
        /// Complete fencing bound.
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
