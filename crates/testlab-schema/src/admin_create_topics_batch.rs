//! Batched topic creation separates ordered scenario expectations from public wire facts.

use serde::{Deserialize, Serialize};

use crate::{ClientId, OperationId};

/// Scenario intent for one ordered public batch topic-creation call.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CreateTopicsBatchAction {
    /// Existing client whose admin handle is used.
    pub client_id: ClientId,
    /// Stable identity for the complete public batch call.
    pub operation_id: OperationId,
    /// Ordered topic requests and their exact expected per-resource outcomes.
    pub topics: Vec<CreateTopicBatchActionItem>,
    /// Complete public operation bound.
    pub timeout_ms: u64,
}

/// One ordered scenario item in a batched topic-creation call.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CreateTopicBatchActionItem {
    /// Exact Kafka topic name.
    pub topic: String,
    /// Initial positive partition count.
    pub partitions: i32,
    /// Initial positive replication factor.
    pub replication_factor: i16,
    /// Exact normalized public error expected for this resource.
    #[serde(default)]
    pub expected_error_code: Option<String>,
}

/// Wire payload for one ordered public batch topic-creation call.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CreateTopicsBatchCommand {
    /// Existing client whose admin handle is used.
    pub client_id: ClientId,
    /// Stable identity for the complete public batch call.
    pub operation_id: OperationId,
    /// Ordered topic requests without verifier-owned expectations.
    pub topics: Vec<CreateTopicBatchCommandItem>,
    /// Complete public operation bound.
    pub timeout_ms: u64,
}

/// One ordered wire item in a batched topic-creation call.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CreateTopicBatchCommandItem {
    /// Exact Kafka topic name.
    pub topic: String,
    /// Initial positive partition count.
    pub partitions: i32,
    /// Initial positive replication factor.
    pub replication_factor: i16,
}

/// Public completion for one ordered batch topic-creation call.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdminTopicsCreationBatch {
    /// Stable identity of the completed public batch call.
    pub operation_id: OperationId,
    /// Ordered per-resource public outcomes.
    pub outcomes: Vec<AdminTopicCreationOutcome>,
}

/// One public per-resource outcome from a batch topic-creation call.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdminTopicCreationOutcome {
    /// Exact topic returned for this request position.
    pub topic: String,
    /// Normalized public broker error, or none on success.
    pub error_code: Option<String>,
}
