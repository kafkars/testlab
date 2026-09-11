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

/// One explicit or broker-relative deletion boundary.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum DeleteRecordsBoundary {
    /// Deletes records below one exact positive offset.
    Offset {
        /// Exclusive deletion boundary.
        offset: i64,
    },
    /// Deletes every currently deletable record below the high watermark.
    HighWatermark,
}

impl DeleteRecordsBoundary {
    /// Resolves the successful low watermark expected from a fixed baseline.
    pub const fn expected_low_watermark(self, high_watermark: i64) -> i64 {
        match self {
            Self::Offset { offset } => offset,
            Self::HighWatermark => high_watermark,
        }
    }
}

/// One scenario-side target for a caller-ordered record-deletion batch.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DeleteRecordsBatchExpectation {
    /// Exact Kafka topic name.
    pub topic: String,
    /// Exact nonnegative partition.
    pub partition: i32,
    /// Explicit or high-watermark deletion boundary.
    pub boundary: DeleteRecordsBoundary,
    /// Independently seeded high watermark before deletion.
    pub expected_high_watermark: i64,
}

/// Scenario intent for one caller-ordered multi-target record deletion.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DeleteRecordsBatchAction {
    /// Existing client whose admin handle is used.
    pub client_id: ClientId,
    /// Stable identity for the complete public call.
    pub operation_id: OperationId,
    /// Caller-ordered distinct deletion targets.
    pub targets: Vec<DeleteRecordsBatchExpectation>,
    /// Complete public operation bound.
    pub timeout_ms: u64,
}

/// One wire-side record-deletion target without verifier expectations.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DeleteRecordsBatchSelection {
    /// Exact Kafka topic name.
    pub topic: String,
    /// Exact nonnegative partition.
    pub partition: i32,
    /// Explicit or high-watermark deletion boundary.
    pub boundary: DeleteRecordsBoundary,
}

/// Wire payload for one caller-ordered multi-target record deletion.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DeleteRecordsBatchCommand {
    /// Existing client whose admin handle is used.
    pub client_id: ClientId,
    /// Stable identity for the complete public call.
    pub operation_id: OperationId,
    /// Caller-ordered distinct deletion targets.
    pub targets: Vec<DeleteRecordsBatchSelection>,
    /// Complete public operation bound.
    pub timeout_ms: u64,
}

/// One public outcome from a caller-ordered record-deletion batch.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdminRecordsDeletionOutcome {
    /// Exact Kafka topic returned for this request position.
    pub topic: String,
    /// Exact partition returned for this request position.
    pub partition: i32,
    /// Public low watermark on success.
    pub low_watermark: Option<i64>,
    /// Stable normalized public error on failure.
    pub error_code: Option<String>,
}

/// Public result for one caller-ordered record-deletion batch.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdminRecordsBatchDeleted {
    /// Stable identity for the complete public call.
    pub operation_id: OperationId,
    /// Caller-ordered public outcomes.
    pub outcomes: Vec<AdminRecordsDeletionOutcome>,
}

#[cfg(test)]
#[path = "admin_delete_records_batch_test.rs"]
mod batch_tests;
