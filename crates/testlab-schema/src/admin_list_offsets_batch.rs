//! Batched offset listing separates caller-ordered expectations from wire facts.

use serde::{Deserialize, Serialize};

use crate::{AdminOffsetPosition, AdminReadIsolation, ClientId, OperationId};

/// Scenario intent for one ordered public batch offset-listing call.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ListOffsetsBatchAction {
    /// Existing client whose admin handle is used.
    pub client_id: ClientId,
    /// Stable identity for the complete public batch call.
    pub operation_id: OperationId,
    /// Transactional visibility for the complete public batch call.
    #[serde(default)]
    pub read_isolation: AdminReadIsolation,
    /// Caller-ordered topic-partition queries and exact expected offsets.
    pub queries: Vec<OffsetListingExpectation>,
    /// Complete public operation bound.
    pub timeout_ms: u64,
}

/// One scenario-side expected offset in a batched listing.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OffsetListingExpectation {
    /// Exact Kafka topic name.
    pub topic: String,
    /// Exact nonnegative partition.
    pub partition: i32,
    /// Broker-relative offset position.
    pub position: AdminOffsetPosition,
    /// Exact nonnegative offset required by the verifier.
    pub expected_offset: i64,
}

/// Wire payload for one ordered public batch offset-listing call.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ListOffsetsBatchCommand {
    /// Existing client whose admin handle is used.
    pub client_id: ClientId,
    /// Stable identity for the complete public batch call.
    pub operation_id: OperationId,
    /// Transactional visibility for the complete public batch call.
    pub read_isolation: AdminReadIsolation,
    /// Caller-ordered queries without verifier-owned expectations.
    pub queries: Vec<OffsetListingSelection>,
    /// Complete public operation bound.
    pub timeout_ms: u64,
}

/// One wire-side topic-partition offset selection.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OffsetListingSelection {
    /// Exact Kafka topic name.
    pub topic: String,
    /// Exact nonnegative partition.
    pub partition: i32,
    /// Broker-relative offset position.
    pub position: AdminOffsetPosition,
}

/// Public result for one ordered batch offset-listing call.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdminOffsetsListing {
    /// Stable identity of the completed public batch call.
    pub operation_id: OperationId,
    /// Caller-ordered per-resource public outcomes.
    pub outcomes: Vec<AdminOffsetListingOutcome>,
}

/// One public per-resource outcome from a batch offset listing.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdminOffsetListingOutcome {
    /// Exact Kafka topic returned for this request position.
    pub topic: String,
    /// Exact partition returned for this request position.
    pub partition: i32,
    /// Reported offset, or absence on per-resource failure.
    pub offset: Option<i64>,
    /// Normalized public error, or none on success.
    pub error_code: Option<String>,
}
