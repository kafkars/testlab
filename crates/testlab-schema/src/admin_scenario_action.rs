//! Admin scenario payloads retain verifier expectations outside wire commands.

use serde::{Deserialize, Serialize};

use crate::{AdminOffsetPosition, ClientId, OperationId};

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
    /// Whether the public API must validate the request without increasing partitions.
    pub validate_only: bool,
    /// Exact current partition count required by the verifier for validate-only requests.
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
    /// Exact partition indices the verifier requires after success.
    pub expected_partitions: Option<Vec<i32>>,
    /// Exact normalized public error expected instead of a completion.
    #[serde(default)]
    pub expected_error_code: Option<String>,
    /// Complete public operation bound.
    pub timeout_ms: u64,
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
    /// Topics the verifier requires in the public result.
    pub required_topics: Vec<String>,
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
    pub position: AdminOffsetPosition,
    /// Exact nonnegative offset the verifier requires after success.
    pub expected_offset: Option<i64>,
    /// Exact normalized public error expected instead of a completion.
    #[serde(default)]
    pub expected_error_code: Option<String>,
    /// Complete public operation bound.
    pub timeout_ms: u64,
}
