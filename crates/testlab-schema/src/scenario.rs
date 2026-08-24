//! Declarative public scenarios are validated before any subject process starts.

use std::collections::BTreeSet;

use crate::{
    BatchRecord, BrokerBehavior, Capability, ClientId, ConsumerId, GroupProtocol,
    OperationAssertion, OperationId, ProducerId, RecordSpec, ScenarioError, ScenarioId,
    ScenarioStep, ShareDisposition, TransactionDisposition,
};
use serde::{Deserialize, Serialize};

/// Current scenario manifest version.
pub const SCENARIO_SCHEMA_VERSION: u16 = 11;

/// One complete black-box scenario.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Scenario {
    /// Exact scenario schema version.
    pub schema_version: u16,
    /// Stable scenario identity.
    pub id: ScenarioId,
    /// Human-readable title.
    pub title: String,
    /// Reviewable statement of intent.
    pub description: String,
    /// Absolute run timeout measured from scenario execution start.
    pub timeout_ms: u64,
    /// Capabilities required from the adapter.
    #[serde(default)]
    pub requires: BTreeSet<Capability>,
    /// Ordered public and environment actions.
    pub steps: Vec<ScenarioStep>,
    /// Deterministic operation assertions.
    pub assertions: Vec<OperationAssertion>,
}

impl Scenario {
    /// Validates structure, identity ownership, and complete lifecycle.
    pub fn validate(&self) -> Result<(), ScenarioError> {
        crate::scenario_validation::validate(self)
    }
}

/// Scenario action vocabulary for scenario schema v11.
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
    /// Registers one unique KIP-932 share-group member.
    CreateShareConsumer {
        /// Existing client that owns the unique member.
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
    },
    /// Retains one exact share batch for a later acknowledgement or drop.
    ShareReceive {
        /// Existing unique share consumer.
        consumer_id: ConsumerId,
        /// Stable retained-batch identity.
        receive_id: OperationId,
        /// Producer operation whose exact record must appear.
        expected_operation_id: OperationId,
        /// Smallest accepted broker delivery count.
        minimum_delivery_count: i16,
        /// Complete public observation bound.
        timeout_ms: u64,
    },
    /// Acknowledges every record in one retained share batch uniformly.
    ShareAcknowledge {
        /// Existing share consumer that owns the broker session.
        consumer_id: ConsumerId,
        /// Retained batch consumed by this acknowledgement.
        receive_id: OperationId,
        /// Stable acknowledgement operation identity.
        acknowledgement_id: OperationId,
        /// Uniform public record disposition.
        disposition: ShareDisposition,
        /// Complete acknowledgement bound.
        timeout_ms: u64,
    },
    /// Drops one retained share batch without a broker acknowledgement.
    DropShareBatch {
        /// Share consumer that produced the retained batch.
        consumer_id: ConsumerId,
        /// Retained batch abandoned without acknowledgement.
        receive_id: OperationId,
    },
    /// Closes one unique share member and declares whether success is required.
    CloseShareConsumer {
        /// Unique share consumer consumed by close.
        consumer_id: ConsumerId,
        /// Whether the public close must report success.
        expect_success: bool,
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
