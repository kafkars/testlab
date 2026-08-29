//! Plural consumer-group offset mutations retain ordered partition outcomes.

use serde::{Deserialize, Serialize};

use crate::{ClientId, ConsumerGroupOffsetSelection, OperationId};

/// One exact committed-offset alteration.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ConsumerGroupOffsetAlteration {
    /// Exact Kafka topic name.
    pub topic: String,
    /// Exact nonnegative partition.
    pub partition: i32,
    /// Exact nonnegative offset to commit.
    pub offset: i64,
}

/// Scenario intent for altering multiple offsets in one consumer group.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AlterConsumerGroupOffsetsAction {
    /// Existing client whose admin handle is used.
    pub client_id: ClientId,
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Exact Kafka consumer-group identity.
    pub group_id: String,
    /// Caller-ordered committed-offset alterations.
    pub offsets: Vec<ConsumerGroupOffsetAlteration>,
    /// Complete public operation bound.
    pub timeout_ms: u64,
}

/// Wire payload for altering multiple offsets in one consumer group.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AlterConsumerGroupOffsetsCommand {
    /// Existing client whose admin handle is used.
    pub client_id: ClientId,
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Exact Kafka consumer-group identity.
    pub group_id: String,
    /// Caller-ordered committed-offset alterations.
    pub offsets: Vec<ConsumerGroupOffsetAlteration>,
    /// Complete public operation bound.
    pub timeout_ms: u64,
}

/// Scenario intent for deleting multiple offsets in one consumer group.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DeleteConsumerGroupOffsetsAction {
    /// Existing client whose admin handle is used.
    pub client_id: ClientId,
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Exact Kafka consumer-group identity.
    pub group_id: String,
    /// Caller-ordered committed-offset selections.
    pub partitions: Vec<ConsumerGroupOffsetSelection>,
    /// Complete public operation bound.
    pub timeout_ms: u64,
}

/// Wire payload for deleting multiple offsets in one consumer group.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DeleteConsumerGroupOffsetsCommand {
    /// Existing client whose admin handle is used.
    pub client_id: ClientId,
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Exact Kafka consumer-group identity.
    pub group_id: String,
    /// Caller-ordered committed-offset selections.
    pub partitions: Vec<ConsumerGroupOffsetSelection>,
    /// Complete public operation bound.
    pub timeout_ms: u64,
}

/// One public partition outcome from a plural offset mutation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdminConsumerGroupOffsetMutationOutcome {
    /// Exact Kafka topic name.
    pub topic: String,
    /// Exact nonnegative partition.
    pub partition: i32,
    /// Stable normalized per-partition error code.
    pub error_code: Option<String>,
}

/// Public result for one plural consumer-group offset mutation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdminConsumerGroupOffsetsMutation {
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Exact Kafka consumer-group identity.
    pub group_id: String,
    /// Caller-ordered public mutation outcomes.
    pub outcomes: Vec<AdminConsumerGroupOffsetMutationOutcome>,
}
