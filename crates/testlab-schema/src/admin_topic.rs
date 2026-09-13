//! Topic-admin payloads separate scenario expectations from wire commands and results.

#[path = "admin_topic_pagination.rs"]
mod pagination;
pub use pagination::{AdminTopicDescriptionPage, AdminTopicPageCursor, TopicDescriptionPagination};

use serde::{Deserialize, Serialize};

use crate::{
    AdminOffsetSelector, AdminTopicDescriptionValue, ClientId, OperationId, TopicDescriptionApi,
};

#[cfg(test)]
#[path = "admin_topic_manual_placement_test.rs"]
mod manual_placement_test;
#[cfg(test)]
#[path = "admin_partition_manual_placement_test.rs"]
mod partition_manual_placement_test;

/// Normalized public error required for a duplicate topic creation.
pub const TOPIC_ALREADY_EXISTS_ERROR_CODE: &str = "broker:broker_36";
/// Normalized Kafka error for a topic or partition that does not exist.
pub const UNKNOWN_TOPIC_OR_PARTITION_ERROR_CODE: &str = "broker:broker_3";
/// Normalized public error when current metadata cannot route an operation.
pub const ROUTING_ERROR_CODE: &str = "routing";

/// One exact partition-to-broker assignment for manual topic creation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TopicReplicaAssignmentSpec {
    /// Zero-based partition index.
    pub partition_index: i32,
    /// Caller-ordered broker IDs for this partition.
    pub broker_ids: Vec<i32>,
}

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
    /// Exact caller-ordered replica placement, or broker-selected placement when absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replica_assignments: Option<Vec<TopicReplicaAssignmentSpec>>,
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
    /// Exact caller-ordered replica placement, or broker-selected placement when absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replica_assignments: Option<Vec<TopicReplicaAssignmentSpec>>,
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
    /// Exact caller-ordered broker IDs for each newly added partition.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replica_assignments: Option<Vec<Vec<i32>>>,
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
    /// Public topic-description operation to execute.
    pub api: TopicDescriptionApi,
    /// Explicit page controls; present only for `DescribeTopicPartitions`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pagination: Option<TopicDescriptionPagination>,
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
    /// Whether Kafka must return the authorized-operation bitfield per listed topic.
    pub include_authorized_operations: bool,
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
    pub position: AdminOffsetSelector,
    /// Caller-selected timestamp for the caller-timestamp selector.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timestamp_millis: Option<i64>,
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
    /// Ordered facts from each separately submitted topic-partition page.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub pages: Vec<AdminTopicDescriptionPage>,
}

/// One canonically positioned outcome from an all-topic listing.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdminListedTopicOutcome {
    /// Exact topic key returned by Kafka.
    pub topic: String,
    /// Full public description on success.
    pub description: Option<AdminTopicDescriptionValue>,
    /// Stable normalized topic error on failure.
    pub error_code: Option<String>,
}

/// Public result for one all-topic listing.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdminTopicsListing {
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Byte-sorted unique topic outcomes reported by the adapter.
    pub outcomes: Vec<AdminListedTopicOutcome>,
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
    /// Timestamp associated with the selected offset, when Kafka reports one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timestamp_millis: Option<i64>,
}
