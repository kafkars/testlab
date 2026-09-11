//! Plural topic-configuration mutation keeps baselines outside the wire command.

use serde::{Deserialize, Serialize};

use crate::{ClientId, OperationId, TopicConfigMutationApi};

/// One scenario-side topic-configuration transition.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AlterTopicConfigExpectation {
    /// Exact Kafka topic name.
    pub topic: String,
    /// Exact configuration key replaced through the public API.
    pub config_name: String,
    /// Exact independently corroborated value before the replacement.
    pub expected_previous_value: String,
    /// Exact replacement value sent through the public API.
    pub value: String,
}

/// Scenario intent for one caller-ordered plural configuration mutation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AlterTopicConfigsAction {
    /// Existing client whose admin handle is used.
    pub client_id: ClientId,
    /// Stable identity for the complete public call.
    pub operation_id: OperationId,
    /// Exact prior plural-description operation used as the baseline.
    pub baseline_operation_id: OperationId,
    /// Public configuration API exercised by the adapter.
    #[serde(default, skip_serializing_if = "is_topic_api")]
    pub api: TopicConfigMutationApi,
    /// Caller-ordered topic-configuration transitions.
    pub topics: Vec<AlterTopicConfigExpectation>,
    /// Complete public operation bound.
    pub timeout_ms: u64,
}

/// One topic-configuration replacement crossing the adapter boundary.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TopicConfigAlteration {
    /// Exact Kafka topic name.
    pub topic: String,
    /// Exact configuration key replaced through the public API.
    pub config_name: String,
    /// Exact replacement value sent through the public API.
    pub value: String,
}

/// Wire payload for one caller-ordered plural configuration mutation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AlterTopicConfigsCommand {
    /// Existing client whose admin handle is used.
    pub client_id: ClientId,
    /// Stable identity for the complete public call.
    pub operation_id: OperationId,
    /// Public configuration API exercised by the adapter.
    #[serde(default, skip_serializing_if = "is_topic_api")]
    pub api: TopicConfigMutationApi,
    /// Caller-ordered replacements without baseline expectations.
    pub topics: Vec<TopicConfigAlteration>,
    /// Complete public operation bound.
    pub timeout_ms: u64,
}

/// One caller-positioned public configuration-mutation outcome.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdminTopicConfigAlterationOutcome {
    /// Exact topic key returned for this request position.
    pub topic: String,
    /// Exact selected configuration key.
    pub config_name: String,
    /// Stable normalized per-topic error when the replacement failed.
    pub error_code: Option<String>,
}

/// Public result for one caller-ordered plural configuration mutation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdminTopicConfigsAlteration {
    /// Stable identity for the complete public call.
    pub operation_id: OperationId,
    /// Caller-ordered public mutation outcomes.
    pub outcomes: Vec<AdminTopicConfigAlterationOutcome>,
}

#[cfg(test)]
#[path = "admin_config_batch_mutation_test.rs"]
mod test;

fn is_topic_api(api: &TopicConfigMutationApi) -> bool {
    *api == TopicConfigMutationApi::Topic
}
