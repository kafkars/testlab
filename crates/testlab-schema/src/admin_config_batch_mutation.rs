//! Plural topic-configuration mutation keeps baselines outside the wire command.

use serde::{Deserialize, Serialize};

use crate::{ClientId, OperationId, TopicConfigMutationApi};

/// Exact public configuration-mutation constructor selected for one key.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TopicConfigMutationMethod {
    /// Replaces the current value.
    #[default]
    Set,
    /// Removes the explicit value and restores the effective default.
    Delete,
    /// Appends one operand using Kafka's list-configuration semantics.
    Append,
    /// Subtracts one operand using Kafka's list-configuration semantics.
    Subtract,
    /// Restores a default through the legacy full-snapshot API.
    RestoreDefault,
}

/// One scenario-side topic-configuration transition.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AlterTopicConfigExpectation {
    /// Exact Kafka topic name.
    pub topic: String,
    /// Exact configuration key changed through the public API.
    pub config_name: String,
    /// Exact independently corroborated value before the mutation.
    pub expected_previous_value: String,
    /// Exact final value required from independent broker observation.
    pub value: String,
    /// Exact public mutation constructor selected by the scenario.
    #[serde(default, skip_serializing_if = "is_set_method")]
    pub method: TopicConfigMutationMethod,
    /// Exact Append or Subtract operand, never the expected final value.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation_value: Option<String>,
}

impl AlterTopicConfigExpectation {
    /// Returns only the value allowed to cross the adapter boundary.
    pub fn command_value(&self) -> Option<&str> {
        match self.method {
            TopicConfigMutationMethod::Set => Some(&self.value),
            TopicConfigMutationMethod::Append | TopicConfigMutationMethod::Subtract => {
                self.operation_value.as_deref()
            }
            TopicConfigMutationMethod::Delete | TopicConfigMutationMethod::RestoreDefault => None,
        }
    }
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

/// One topic-configuration mutation crossing the adapter boundary.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TopicConfigAlteration {
    /// Exact Kafka topic name.
    pub topic: String,
    /// Exact configuration key changed through the public API.
    pub config_name: String,
    /// Exact public mutation constructor selected by the harness.
    #[serde(default, skip_serializing_if = "is_set_method")]
    pub method: TopicConfigMutationMethod,
    /// Exact replacement or list operand, absent for deletion or restoration.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
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
    /// Caller-ordered mutations without baseline expectations.
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
    /// Stable normalized per-topic error when the mutation failed.
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

fn is_set_method(method: &TopicConfigMutationMethod) -> bool {
    *method == TopicConfigMutationMethod::Set
}
