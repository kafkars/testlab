//! Plural topic descriptions keep expectations outside caller-ordered wire results.

use serde::{Deserialize, Serialize};

use crate::{ClientId, OperationId};

/// Public Kafka topic key used by one plural description or deletion call.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TopicSelection {
    /// Address topics by their names.
    #[default]
    Name,
    /// Resolve scenario-owned names once, then address topics by Kafka UUID.
    TopicId,
}

impl TopicSelection {
    /// Returns whether the public request uses topic names.
    pub const fn is_name(&self) -> bool {
        matches!(self, Self::Name)
    }
}

/// One scenario-side topic description expectation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DescribeTopicExpectation {
    /// Exact Kafka topic name.
    pub topic: String,
    /// Exact sorted partition indices required after success.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_partitions: Option<Vec<i32>>,
    /// Exact normalized public per-topic error required instead of a description.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_error_code: Option<String>,
}

/// Scenario intent for one caller-ordered plural topic-description call.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DescribeTopicsAction {
    /// Existing client whose admin handle is used.
    pub client_id: ClientId,
    /// Stable identity for the complete public call.
    pub operation_id: OperationId,
    /// Public topic key used after scenario-owned resource resolution.
    #[serde(default, skip_serializing_if = "TopicSelection::is_name")]
    pub selection: TopicSelection,
    /// Caller-ordered topic expectations.
    pub topics: Vec<DescribeTopicExpectation>,
    /// Whether Kafka must return the authorized-operation bitfield per topic.
    pub include_authorized_operations: bool,
    /// Complete public operation bound.
    pub timeout_ms: u64,
}

/// Wire payload for one caller-ordered plural topic-description call.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DescribeTopicsCommand {
    /// Existing client whose admin handle is used.
    pub client_id: ClientId,
    /// Stable identity for the complete public call.
    pub operation_id: OperationId,
    /// Public topic key used after scenario-owned resource resolution.
    #[serde(default, skip_serializing_if = "TopicSelection::is_name")]
    pub selection: TopicSelection,
    /// Caller-ordered topic names without verifier expectations.
    pub topics: Vec<String>,
    /// Whether Kafka must return the authorized-operation bitfield per topic.
    pub include_authorized_operations: bool,
    /// Complete public operation bound.
    pub timeout_ms: u64,
}

/// One public partition description retained inside a topic outcome.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdminTopicPartitionDescriptionOutcome {
    /// Exact nonnegative partition index.
    pub partition: i32,
    /// Stable normalized partition error, when Kafka supplied one.
    pub error_code: Option<String>,
}

/// One successful public topic description.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdminTopicDescriptionValue {
    /// Kafka topic identity when supplied by the negotiated version.
    pub topic_id: Option<[u8; 16]>,
    /// Whether Kafka marks the topic as internal.
    pub internal: bool,
    /// Raw Kafka authorization bitfield, when requested.
    pub authorized_operations: Option<i32>,
    /// Public partition descriptions in returned order.
    pub partitions: Vec<AdminTopicPartitionDescriptionOutcome>,
}

/// One caller-positioned public topic outcome.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdminTopicDescriptionOutcome {
    /// Exact topic key returned for this request position.
    pub topic: String,
    /// Exact nonzero topic-ID request key, or none for a name-based request.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topic_id: Option<[u8; 16]>,
    /// Full public description on success.
    pub description: Option<AdminTopicDescriptionValue>,
    /// Stable normalized topic error on failure.
    pub error_code: Option<String>,
}

/// Public result for one caller-ordered plural topic-description call.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdminTopicsDescription {
    /// Stable identity for the complete public call.
    pub operation_id: OperationId,
    /// Caller-ordered public topic outcomes.
    pub outcomes: Vec<AdminTopicDescriptionOutcome>,
}
