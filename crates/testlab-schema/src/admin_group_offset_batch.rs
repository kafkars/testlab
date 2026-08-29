//! Plural consumer-group offset listings preserve ordered public outcomes.

use serde::{Deserialize, Serialize};

use crate::{ClientId, OperationId};

/// One scenario-side expected committed offset.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ConsumerGroupOffsetExpectation {
    /// Exact Kafka topic name.
    pub topic: String,
    /// Exact nonnegative partition.
    pub partition: i32,
    /// Exact nonnegative committed offset required by the verifier.
    pub expected_offset: i64,
}

/// One wire-side committed-offset selection.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ConsumerGroupOffsetSelection {
    /// Exact Kafka topic name.
    pub topic: String,
    /// Exact nonnegative partition.
    pub partition: i32,
}

/// Scenario intent for listing multiple offsets from one consumer group.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ListConsumerGroupOffsetsBatchAction {
    /// Existing client whose admin handle is used.
    pub client_id: ClientId,
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Exact Kafka consumer-group identity.
    pub group_id: String,
    /// Whether the public request requires a stable group state.
    pub require_stable: bool,
    /// Caller-ordered committed-offset expectations.
    pub partitions: Vec<ConsumerGroupOffsetExpectation>,
    /// Complete public operation bound.
    pub timeout_ms: u64,
}

/// Wire payload for listing multiple offsets from one consumer group.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ListConsumerGroupOffsetsBatchCommand {
    /// Existing client whose admin handle is used.
    pub client_id: ClientId,
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Exact Kafka consumer-group identity.
    pub group_id: String,
    /// Whether the public request requires a stable group state.
    pub require_stable: bool,
    /// Caller-ordered committed-offset selections.
    pub partitions: Vec<ConsumerGroupOffsetSelection>,
    /// Complete public operation bound.
    pub timeout_ms: u64,
}

/// One group-scoped scenario expectation for plural offset listing.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ConsumerGroupOffsetsExpectation {
    /// Exact Kafka consumer-group identity.
    pub group_id: String,
    /// Caller-ordered expected offsets for this group.
    pub partitions: Vec<ConsumerGroupOffsetExpectation>,
}

/// One group-scoped wire selection for plural offset listing.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ConsumerGroupOffsetsSelection {
    /// Exact Kafka consumer-group identity.
    pub group_id: String,
    /// Caller-ordered selected partitions for this group.
    pub partitions: Vec<ConsumerGroupOffsetSelection>,
}

/// Scenario intent for listing selected offsets from multiple groups.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ListConsumerGroupsOffsetsAction {
    /// Existing client whose admin handle is used.
    pub client_id: ClientId,
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Whether the public request requires a stable group state.
    pub require_stable: bool,
    /// Caller-ordered group expectations.
    pub groups: Vec<ConsumerGroupOffsetsExpectation>,
    /// Complete public operation bound.
    pub timeout_ms: u64,
}

/// Wire payload for listing selected offsets from multiple groups.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ListConsumerGroupsOffsetsCommand {
    /// Existing client whose admin handle is used.
    pub client_id: ClientId,
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Whether the public request requires a stable group state.
    pub require_stable: bool,
    /// Caller-ordered group selections.
    pub groups: Vec<ConsumerGroupOffsetsSelection>,
    /// Complete public operation bound.
    pub timeout_ms: u64,
}

/// One public committed-offset outcome.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdminConsumerGroupOffsetOutcome {
    /// Exact Kafka topic name.
    pub topic: String,
    /// Exact nonnegative partition.
    pub partition: i32,
    /// Reported committed offset, or absence when Kafka has no value.
    pub offset: Option<i64>,
    /// Stable normalized per-partition error code.
    pub error_code: Option<String>,
}

/// Public result for selected offsets from one consumer group.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdminConsumerGroupOffsetsListing {
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Exact Kafka consumer-group identity.
    pub group_id: String,
    /// Caller-ordered public outcomes.
    pub outcomes: Vec<AdminConsumerGroupOffsetOutcome>,
}

/// One group-scoped public outcome from a plural listing.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdminConsumerGroupOffsetsOutcome {
    /// Exact Kafka consumer-group identity.
    pub group_id: String,
    /// Stable normalized group-level error code.
    pub error_code: Option<String>,
    /// Caller-ordered partition outcomes.
    pub offsets: Vec<AdminConsumerGroupOffsetOutcome>,
}

/// Public result for selected offsets from multiple consumer groups.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdminConsumerGroupsOffsetsListing {
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Caller-ordered group outcomes.
    pub groups: Vec<AdminConsumerGroupOffsetsOutcome>,
}
