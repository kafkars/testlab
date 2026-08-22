//! Adapter commands are the public operations testctl may request.

use serde::{Deserialize, Serialize};

use crate::{
    AdapterSecurity, BatchRecord, ClientId, ConsumerId, OperationId, ProducerId, RecordSpec, RunId,
    ScenarioId,
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
    /// Registers one classic consumer-group member.
    CreateGroupConsumer {
        /// Owning client.
        client_id: ClientId,
        /// Scenario-local consumer identity.
        consumer_id: ConsumerId,
        /// Exact Kafka group identity.
        group_id: String,
        /// Subscribed topic.
        topic: String,
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
