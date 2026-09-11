//! Plural Share-group offset listings preserve ordered public outcomes.

use serde::{Deserialize, Serialize};

use crate::{ClientId, OperationId};

/// One scenario-side expected Share-group partition offset.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ShareGroupOffsetExpectation {
    /// Exact Kafka topic name.
    pub topic: String,
    /// Exact nonnegative partition.
    pub partition: i32,
    /// Exact nonnegative start offset required by the verifier.
    pub expected_start_offset: i64,
    /// Exact nonnegative lag required by the verifier.
    pub expected_lag: i64,
}

/// One wire-side Share-group partition selection.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ShareGroupOffsetSelection {
    /// Exact Kafka topic name.
    pub topic: String,
    /// Exact nonnegative partition.
    pub partition: i32,
}

/// One group-scoped scenario expectation for plural offset listing.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ShareGroupOffsetsExpectation {
    /// Exact Kafka Share-group identity.
    pub group_id: String,
    /// Caller-ordered expected offsets for this group.
    pub partitions: Vec<ShareGroupOffsetExpectation>,
}

/// One group-scoped wire selection for plural offset listing.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ShareGroupOffsetsSelection {
    /// Exact Kafka Share-group identity.
    pub group_id: String,
    /// Caller-ordered selected partitions for this group.
    pub partitions: Vec<ShareGroupOffsetSelection>,
}

/// Scenario intent for listing selected offsets from multiple Share groups.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ListShareGroupsOffsetsAction {
    /// Existing client whose admin handle is used.
    pub client_id: ClientId,
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Caller-ordered Share-group expectations.
    pub groups: Vec<ShareGroupOffsetsExpectation>,
    /// Complete public operation bound.
    pub timeout_ms: u64,
}

/// Wire payload for listing selected offsets from multiple Share groups.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ListShareGroupsOffsetsCommand {
    /// Existing client whose admin handle is used.
    pub client_id: ClientId,
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Caller-ordered Share-group selections.
    pub groups: Vec<ShareGroupOffsetsSelection>,
    /// Complete public operation bound.
    pub timeout_ms: u64,
}

/// One public Share-group partition offset outcome.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdminShareGroupOffsetOutcome {
    /// Exact Kafka topic name.
    pub topic: String,
    /// Exact nonnegative partition.
    pub partition: i32,
    /// Broker-issued nonzero topic identity on success.
    pub topic_id: [u8; 16],
    /// Share-partition start offset, when Kafka supplied one.
    pub start_offset: Option<i64>,
    /// Partition leader epoch, when Kafka supplied one.
    pub leader_epoch: Option<i32>,
    /// Share-partition lag, when the negotiated version supplied it.
    pub lag: Option<i64>,
    /// Stable normalized per-partition error code.
    pub error_code: Option<String>,
}

/// One group-scoped public outcome from a plural Share offset listing.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdminShareGroupOffsetsOutcome {
    /// Exact Kafka Share-group identity.
    pub group_id: String,
    /// Stable normalized group-level error code.
    pub error_code: Option<String>,
    /// Caller-ordered partition outcomes.
    pub offsets: Vec<AdminShareGroupOffsetOutcome>,
}

/// Public result for selected offsets from multiple Share groups.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdminShareGroupsOffsetsListing {
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Caller-ordered Share-group outcomes.
    pub groups: Vec<AdminShareGroupOffsetsOutcome>,
}
