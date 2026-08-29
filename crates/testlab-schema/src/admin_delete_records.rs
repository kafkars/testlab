//! Delete-records payloads separate verifier expectations from public wire facts.

use serde::{Deserialize, Serialize};

use crate::{ClientId, OperationId};

/// Scenario intent for one bounded prefix deletion on an exact partition.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DeleteRecordsAction {
    /// Existing client whose admin handle is used.
    pub client_id: ClientId,
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Exact Kafka topic name.
    pub topic: String,
    /// Exact nonnegative partition.
    pub partition: i32,
    /// Exclusive deletion boundary.
    pub before_offset: i64,
    /// Pre-deletion high watermark required by the verifier.
    pub expected_high_watermark: i64,
    /// Complete public operation bound.
    pub timeout_ms: u64,
}

/// Wire payload for one bounded prefix deletion on an exact partition.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DeleteRecordsCommand {
    /// Existing client whose admin handle is used.
    pub client_id: ClientId,
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Exact Kafka topic name.
    pub topic: String,
    /// Exact nonnegative partition.
    pub partition: i32,
    /// Exclusive deletion boundary.
    pub before_offset: i64,
    /// Complete public operation bound.
    pub timeout_ms: u64,
}

/// Public completion for one exact prefix deletion.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdminRecordsDeleted {
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Exact Kafka topic name.
    pub topic: String,
    /// Exact nonnegative partition.
    pub partition: i32,
    /// Low watermark reported by the public admin result.
    pub low_watermark: i64,
}
