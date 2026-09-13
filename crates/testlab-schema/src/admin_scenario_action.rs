//! Admin scenario payloads retain verifier expectations outside wire commands.

use serde::{Deserialize, Serialize};

use crate::{AdminOffsetSelector, ClientId, OperationId, TopicDescriptionPagination};

/// Public topic-description operation selected by a scenario.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TopicDescriptionApi {
    /// Uses the client's complete metadata-backed topic description.
    #[default]
    Metadata,
    /// Uses explicitly configured `DescribeTopicPartitions` response pages.
    DescribeTopicPartitions,
}

/// Payload for one declarative partition-count increase action.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CreatePartitionsAction {
    /// Existing client whose admin handle is used.
    pub client_id: ClientId,
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Exact Kafka topic name.
    pub topic: String,
    /// Positive requested total partition count.
    pub total_count: i32,
    /// Exact caller-ordered broker IDs for each newly added partition.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replica_assignments: Option<Vec<Vec<i32>>>,
    /// Whether the public API must validate the request without increasing partitions.
    pub validate_only: bool,
    /// Exact current count required for validate-only or manual-placement requests.
    pub expected_current_count: Option<i32>,
    /// Exact normalized public error expected instead of a completion.
    #[serde(default)]
    pub expected_error_code: Option<String>,
    /// Complete public operation bound.
    pub timeout_ms: u64,
}

/// Payload for one declarative topic-description action.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DescribeTopicAction {
    /// Existing client whose admin handle is used.
    pub client_id: ClientId,
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Exact Kafka topic name.
    pub topic: String,
    /// Public topic-description operation exercised by the adapter.
    #[serde(default)]
    pub api: TopicDescriptionApi,
    /// Explicit page controls; present only for `DescribeTopicPartitions`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pagination: Option<TopicDescriptionPagination>,
    /// Exact partition indices the verifier requires after success.
    pub expected_partitions: Option<Vec<i32>>,
    /// Exact partition boundaries required for every explicit public page.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_page_partitions: Option<Vec<Vec<i32>>>,
    /// Exact normalized public error expected instead of a completion.
    #[serde(default)]
    pub expected_error_code: Option<String>,
    /// Complete public operation bound.
    pub timeout_ms: u64,
}

/// One independently observed topic expectation for an all-topic listing.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TopicListingExpectation {
    /// Exact Kafka topic name.
    pub topic: String,
    /// Whether Kafka classifies this topic as internal.
    pub internal: bool,
    /// Whether this topic must enter the public result.
    pub included: bool,
}

/// Payload for one declarative topic-listing action.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ListTopicsAction {
    /// Existing client whose admin handle is used.
    pub client_id: ClientId,
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Whether broker-marked internal topics enter the public result.
    pub include_internal: bool,
    /// Whether Kafka must return the authorized-operation bitfield per listed topic.
    pub include_authorized_operations: bool,
    /// Independently observed topics with exact public inclusion expectations.
    pub expected_topics: Vec<TopicListingExpectation>,
    /// Complete public operation bound.
    pub timeout_ms: u64,
}

/// Payload for one declarative offset-listing action.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ListOffsetsAction {
    /// Existing client whose admin handle is used.
    pub client_id: ClientId,
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Exact Kafka topic name.
    pub topic: String,
    /// Exact nonnegative partition.
    pub partition: i32,
    /// Broker-relative offset position.
    pub position: AdminOffsetSelector,
    /// Transactional visibility for this public offset query.
    #[serde(default)]
    pub read_isolation: crate::AdminReadIsolation,
    /// Caller-selected timestamp for the caller-timestamp selector.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timestamp_millis: Option<i64>,
    /// Exact nonnegative offset the verifier requires after success.
    pub expected_offset: Option<i64>,
    /// Exact normalized public error expected instead of a completion.
    #[serde(default)]
    pub expected_error_code: Option<String>,
    /// Complete public operation bound.
    pub timeout_ms: u64,
}
