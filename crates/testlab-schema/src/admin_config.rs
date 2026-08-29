//! Topic-configuration admin payloads separate expectations from public commands.

use serde::{Deserialize, Serialize};

use crate::{ClientId, OperationId};

/// Scenario intent for one bounded topic-configuration description.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DescribeTopicConfigAction {
    /// Existing client whose admin handle is used.
    pub client_id: ClientId,
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Exact Kafka topic name.
    pub topic: String,
    /// Exact configuration key selected through the public API.
    pub config_name: String,
    /// Exact non-sensitive value required by the verifier.
    pub expected_value: String,
    /// Complete public operation bound.
    pub timeout_ms: u64,
}

/// Wire payload for one bounded topic-configuration description.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DescribeTopicConfigCommand {
    /// Existing client whose admin handle is used.
    pub client_id: ClientId,
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Exact Kafka topic name.
    pub topic: String,
    /// Exact configuration key selected through the public API.
    pub config_name: String,
    /// Complete public operation bound.
    pub timeout_ms: u64,
}

/// Scenario intent for one bounded incremental topic-configuration replacement.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AlterTopicConfigAction {
    /// Existing client whose admin handle is used.
    pub client_id: ClientId,
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Exact Kafka topic name.
    pub topic: String,
    /// Exact configuration key replaced through the public API.
    pub config_name: String,
    /// Exact replacement value sent through the public API.
    pub value: String,
    /// Whether the public API must validate the request without changing the configuration.
    pub validate_only: bool,
    /// Exact current value required by the verifier for validate-only requests.
    pub expected_current_value: Option<String>,
    /// Complete public operation bound.
    pub timeout_ms: u64,
}

/// Wire payload for one bounded incremental topic-configuration replacement.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AlterTopicConfigCommand {
    /// Existing client whose admin handle is used.
    pub client_id: ClientId,
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Exact Kafka topic name.
    pub topic: String,
    /// Exact configuration key replaced through the public API.
    pub config_name: String,
    /// Exact replacement value sent through the public API.
    pub value: String,
    /// Whether the public API validates the request without changing the configuration.
    pub validate_only: bool,
    /// Complete public operation bound.
    pub timeout_ms: u64,
}

/// Public result for one exact topic-configuration description.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdminTopicConfigDescription {
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Exact Kafka topic name.
    pub topic: String,
    /// Exact configuration key returned by the public API.
    pub config_name: String,
    /// Public value, preserving absence for sensitive or unavailable values.
    pub value: Option<String>,
}

/// Public completion for one exact topic-configuration mutation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdminTopicConfigCompletion {
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Exact Kafka topic name.
    pub topic: String,
    /// Exact configuration key mutated through the public API.
    pub config_name: String,
}

/// Independently observed value for one exact topic configuration.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BrokerTopicConfigState {
    /// Monotonic broker-state observation identity.
    pub observation: u64,
    /// Public operation whose result triggered this query.
    pub operation_id: OperationId,
    /// Exact Kafka topic name.
    pub topic: String,
    /// Exact configuration key queried independently.
    pub config_name: String,
    /// Independently returned non-sensitive value.
    pub value: String,
}
