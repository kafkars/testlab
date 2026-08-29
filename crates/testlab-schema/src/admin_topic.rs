//! Topic-admin payloads separate scenario expectations from wire commands and results.

use serde::{Deserialize, Serialize};

use crate::{AdminOffsetPosition, ClientId, OperationId};

/// Normalized public error required for a duplicate topic creation.
pub const TOPIC_ALREADY_EXISTS_ERROR_CODE: &str = "broker:broker_36";
/// Normalized Kafka error for a topic or partition that does not exist.
pub const UNKNOWN_TOPIC_OR_PARTITION_ERROR_CODE: &str = "broker:broker_3";

/// Scenario intent for one bounded topic creation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CreateTopicAction {
    /// Existing client whose admin handle is used.
    pub client_id: ClientId,
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Exact Kafka topic name.
    pub topic: String,
    /// Initial positive partition count.
    pub partitions: i32,
    /// Initial positive replication factor.
    pub replication_factor: i16,
    /// Whether the public API must validate the request without creating the topic.
    pub validate_only: bool,
    /// Exact normalized public error expected instead of a completion.
    #[serde(default)]
    pub expected_error_code: Option<String>,
    /// Complete public operation bound.
    pub timeout_ms: u64,
}

/// Wire payload for one bounded topic creation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CreateTopicCommand {
    /// Existing client whose admin handle is used.
    pub client_id: ClientId,
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Exact Kafka topic name.
    pub topic: String,
    /// Initial positive partition count.
    pub partitions: i32,
    /// Initial positive replication factor.
    pub replication_factor: i16,
    /// Whether the public API validates the request without creating the topic.
    pub validate_only: bool,
    /// Complete public operation bound.
    pub timeout_ms: u64,
}

/// Wire payload for one bounded partition-count increase.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CreatePartitionsCommand {
    /// Existing client whose admin handle is used.
    pub client_id: ClientId,
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Exact Kafka topic name.
    pub topic: String,
    /// Requested positive total partition count.
    pub total_count: i32,
    /// Whether the public API validates the request without increasing partitions.
    pub validate_only: bool,
    /// Complete public operation bound.
    pub timeout_ms: u64,
}

/// Scenario intent for one bounded topic deletion.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DeleteTopicAction {
    /// Existing client whose admin handle is used.
    pub client_id: ClientId,
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Exact Kafka topic name.
    pub topic: String,
    /// Exact normalized public error expected instead of a completion.
    #[serde(default)]
    pub expected_error_code: Option<String>,
    /// Complete public operation bound.
    pub timeout_ms: u64,
}

/// Wire payload for one bounded topic deletion.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DeleteTopicCommand {
    /// Existing client whose admin handle is used.
    pub client_id: ClientId,
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Exact Kafka topic name.
    pub topic: String,
    /// Complete public operation bound.
    pub timeout_ms: u64,
}

/// Wire payload for one bounded topic description.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DescribeTopicCommand {
    /// Existing client whose admin handle is used.
    pub client_id: ClientId,
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Exact Kafka topic name.
    pub topic: String,
    /// Complete public operation bound.
    pub timeout_ms: u64,
}

/// Wire payload for one bounded topic listing.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ListTopicsCommand {
    /// Existing client whose admin handle is used.
    pub client_id: ClientId,
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Whether broker-internal topics are included.
    pub include_internal: bool,
    /// Complete public operation bound.
    pub timeout_ms: u64,
}

/// Wire payload for one bounded offset listing.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ListOffsetsCommand {
    /// Existing client whose admin handle is used.
    pub client_id: ClientId,
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Exact Kafka topic name.
    pub topic: String,
    /// Exact nonnegative partition.
    pub partition: i32,
    /// Broker-relative offset position to query.
    pub position: AdminOffsetPosition,
    /// Complete public operation bound.
    pub timeout_ms: u64,
}

/// Public completion for one exact topic mutation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdminTopicCompletion {
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Exact topic reported as mutated.
    pub topic: String,
}

/// Public result for one exact topic description.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdminTopicDescription {
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Exact topic reported as described.
    pub topic: String,
    /// Sorted partition identifiers reported by the adapter.
    pub partitions: Vec<i32>,
}

/// Public result for one all-topic listing.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdminTopicsListing {
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Sorted topic names reported by the adapter.
    pub topics: Vec<String>,
}

/// Public result for one exact topic-partition offset.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdminOffsetListing {
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Exact Kafka topic name.
    pub topic: String,
    /// Exact nonnegative partition.
    pub partition: i32,
    /// Reported offset, or no offset when Kafka has no value.
    pub offset: Option<i64>,
}
