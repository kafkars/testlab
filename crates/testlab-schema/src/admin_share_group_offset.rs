//! Share-group offset payloads keep scenario expectations outside adapter commands.

use serde::{Deserialize, Serialize};

use crate::{ClientId, OperationId};

/// Scenario intent for one selected Share-group partition offset listing.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ListShareGroupOffsetsAction {
    /// Existing client whose admin handle is used.
    pub client_id: ClientId,
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Exact Kafka Share-group identity.
    pub group_id: String,
    /// Exact Kafka topic name.
    pub topic: String,
    /// Exact nonnegative partition.
    pub partition: i32,
    /// Exact nonnegative start offset required by the verifier.
    pub expected_start_offset: i64,
    /// Exact nonnegative lag required by the verifier.
    pub expected_lag: i64,
    /// Complete public operation bound.
    pub timeout_ms: u64,
}

/// Wire payload for one selected Share-group partition offset listing.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ListShareGroupOffsetsCommand {
    /// Existing client whose admin handle is used.
    pub client_id: ClientId,
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Exact Kafka Share-group identity.
    pub group_id: String,
    /// Exact Kafka topic name.
    pub topic: String,
    /// Exact nonnegative partition.
    pub partition: i32,
    /// Complete public operation bound.
    pub timeout_ms: u64,
}

/// Public result for one selected Share-group partition offset.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdminShareGroupOffsetListing {
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Exact Kafka Share-group identity.
    pub group_id: String,
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
