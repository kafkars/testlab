//! Plural topic deletion separates caller-ordered expectations from destructive wire input.

use serde::{Deserialize, Serialize};

use crate::{ClientId, OperationId};

/// One scenario-side topic-deletion expectation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DeleteTopicExpectation {
    /// Exact Kafka topic name.
    pub topic: String,
    /// Exact normalized public per-topic error, or success when absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_error_code: Option<String>,
}

/// Scenario intent for one caller-ordered plural topic deletion.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DeleteTopicsAction {
    /// Existing client whose admin handle is used.
    pub client_id: ClientId,
    /// Stable identity for the complete public call.
    pub operation_id: OperationId,
    /// Caller-ordered topic expectations.
    pub topics: Vec<DeleteTopicExpectation>,
    /// Complete public operation bound.
    pub timeout_ms: u64,
}

/// Wire payload for one caller-ordered plural topic deletion.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DeleteTopicsCommand {
    /// Existing client whose admin handle is used.
    pub client_id: ClientId,
    /// Stable identity for the complete public call.
    pub operation_id: OperationId,
    /// Caller-ordered topic names without verifier expectations.
    pub topics: Vec<String>,
    /// Complete public operation bound.
    pub timeout_ms: u64,
}

/// One caller-positioned public topic-deletion outcome.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdminTopicDeletionOutcome {
    /// Exact topic key returned for this request position.
    pub topic: String,
    /// Stable normalized public error, or none on success.
    pub error_code: Option<String>,
}

/// Public result for one caller-ordered plural topic deletion.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdminTopicsDeletion {
    /// Stable identity for the complete public call.
    pub operation_id: OperationId,
    /// Caller-ordered public outcomes.
    pub outcomes: Vec<AdminTopicDeletionOutcome>,
}
