//! Topic-configuration admin payloads separate expectations from public commands.

use serde::{Deserialize, Serialize};

use crate::{ClientId, OperationId};

/// Public configuration API selected for a plural topic-resource request.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TopicConfigApi {
    /// Uses the topic-specific convenience surface.
    #[default]
    Topic,
    /// Uses the resource-generic configuration surface with topic resources.
    Resource,
}

impl TopicConfigApi {
    fn is_topic(&self) -> bool {
        *self == Self::Topic
    }
}

/// Public configuration-mutation API selected for a plural topic-resource request.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TopicConfigMutationApi {
    /// Uses the topic-specific incremental-alter convenience surface.
    #[default]
    Topic,
    /// Uses the resource-generic incremental-alter surface with topic resources.
    Resource,
    /// Uses the legacy topic-specific full-snapshot replacement surface.
    LegacyTopic,
    /// Uses the legacy resource-generic full-snapshot replacement surface.
    LegacyResource,
}

impl TopicConfigMutationApi {
    /// Returns the matching public description surface for the mutation baseline.
    pub const fn description_api(self) -> TopicConfigApi {
        match self {
            Self::Topic | Self::LegacyTopic => TopicConfigApi::Topic,
            Self::Resource | Self::LegacyResource => TopicConfigApi::Resource,
        }
    }
}

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

/// One scenario-side selected topic-configuration expectation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DescribeTopicConfigExpectation {
    /// Exact Kafka topic name.
    pub topic: String,
    /// Exact configuration key selected for this topic.
    pub config_name: String,
    /// Exact non-sensitive value required by the verifier.
    pub expected_value: String,
}

/// Scenario intent for one caller-ordered plural topic-configuration description.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DescribeTopicConfigsAction {
    /// Existing client whose admin handle is used.
    pub client_id: ClientId,
    /// Stable identity for the complete public call.
    pub operation_id: OperationId,
    /// Public configuration API exercised by the adapter.
    #[serde(default, skip_serializing_if = "TopicConfigApi::is_topic")]
    pub api: TopicConfigApi,
    /// Caller-ordered selected topic-configuration expectations.
    pub topics: Vec<DescribeTopicConfigExpectation>,
    /// Complete public operation bound.
    pub timeout_ms: u64,
}

/// One selected topic configuration crossing the adapter boundary.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TopicConfigSelection {
    /// Exact Kafka topic name.
    pub topic: String,
    /// Exact configuration key selected for this topic.
    pub config_name: String,
}

/// Wire payload for one caller-ordered plural topic-configuration description.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DescribeTopicConfigsCommand {
    /// Existing client whose admin handle is used.
    pub client_id: ClientId,
    /// Stable identity for the complete public call.
    pub operation_id: OperationId,
    /// Public configuration API exercised by the adapter.
    #[serde(default, skip_serializing_if = "TopicConfigApi::is_topic")]
    pub api: TopicConfigApi,
    /// Caller-ordered selected topic configurations without expectations.
    pub topics: Vec<TopicConfigSelection>,
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

/// One caller-positioned public topic-configuration outcome.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdminTopicConfigDescriptionOutcome {
    /// Exact topic key returned for this request position.
    pub topic: String,
    /// Exact selected configuration key.
    pub config_name: String,
    /// Public value, preserving absence for sensitive or unavailable values.
    pub value: Option<String>,
    /// Stable normalized per-topic error when the selected description failed.
    pub error_code: Option<String>,
}

/// Public result for one caller-ordered plural topic-configuration description.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdminTopicConfigsDescription {
    /// Stable identity for the complete public call.
    pub operation_id: OperationId,
    /// Caller-ordered public topic-configuration outcomes.
    pub outcomes: Vec<AdminTopicConfigDescriptionOutcome>,
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

#[cfg(test)]
#[path = "admin_config_batch_test.rs"]
mod batch_test;
#[cfg(test)]
#[path = "admin_config_test.rs"]
mod test;
