//! Independent broker-state payloads retain bounded facts tied to admin operations.

use serde::{Deserialize, Serialize};

use crate::OperationId;

/// Topic existence and partition indices read independently from Kafka metadata.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BrokerTopicState {
    /// Monotonic observation identity within the run.
    pub observation: u64,
    /// Admin operation whose result triggered this observation.
    pub operation_id: OperationId,
    /// Exact Kafka topic name.
    pub topic: String,
    /// Whether the topic exists in broker metadata.
    pub exists: bool,
    /// Sorted broker-visible partition identifiers.
    pub partitions: Vec<i32>,
}

/// Cluster identity and broker set read independently from Kafka metadata.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BrokerClusterState {
    /// Monotonic observation identity within the run.
    pub observation: u64,
    /// Admin operation whose result triggered this observation.
    pub operation_id: OperationId,
    /// Independently observed Kafka cluster identity, when available.
    pub cluster_id: Option<String>,
    /// Sorted independently observed broker identifiers.
    pub broker_ids: Vec<i32>,
}

/// Consumer-group existence and membership read independently from Kafka.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BrokerConsumerGroupState {
    /// Monotonic observation identity within the run.
    pub observation: u64,
    /// Admin operation whose result triggered this observation.
    pub operation_id: OperationId,
    /// Exact Kafka consumer-group identity.
    pub group_id: String,
    /// Whether the group exists in the broker result.
    pub exists: bool,
    /// Independently observed member count, when the group exists.
    pub member_count: Option<u32>,
}

/// One committed consumer-group offset independently read from Kafka.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BrokerConsumerGroupOffset {
    /// Monotonic observation identity within the run.
    pub observation: u64,
    /// Admin operation whose result triggered this observation.
    pub operation_id: OperationId,
    /// Exact Kafka consumer-group identity.
    pub group_id: String,
    /// Exact Kafka topic name.
    pub topic: String,
    /// Exact nonnegative partition.
    pub partition: i32,
    /// Independently observed committed offset, or no committed value.
    pub offset: Option<i64>,
}

/// One partition watermark pair independently read from Kafka.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BrokerPartitionOffsets {
    /// Monotonic observation identity within the run.
    pub observation: u64,
    /// Admin operation whose result triggered this observation.
    pub operation_id: OperationId,
    /// Exact Kafka topic name.
    pub topic: String,
    /// Exact nonnegative partition.
    pub partition: i32,
    /// Independently observed low watermark.
    pub low_watermark: i64,
    /// Independently observed high watermark.
    pub high_watermark: i64,
}
