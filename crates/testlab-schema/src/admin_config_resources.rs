//! Generic configuration-resource listing keeps scenario expectations off the wire.

use serde::{Deserialize, Serialize};

use crate::{ClientId, OperationId};

/// Public listing surface selected for one configuration-resource call.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ConfigResourceListingApi {
    /// Kafka API 74 v1 generic resource-type listing.
    Resource,
    /// Kafka API 74 v0 dedicated client-metrics resource listing.
    ClientMetrics,
}

/// Scenario intent for one bounded configuration-resource listing.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ListConfigResourcesAction {
    /// Existing client whose admin handle is used.
    pub client_id: ClientId,
    /// Stable identity for the complete public call.
    pub operation_id: OperationId,
    /// Exact public facade selected by the scenario.
    pub api: ConfigResourceListingApi,
    /// Exact resource names required in public and independent results.
    pub required_resources: Vec<String>,
    /// Complete public operation bound.
    pub timeout_ms: u64,
}

/// Wire payload for one bounded configuration-resource listing.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ListConfigResourcesCommand {
    /// Existing client whose admin handle is used.
    pub client_id: ClientId,
    /// Stable identity for the complete public call.
    pub operation_id: OperationId,
    /// Exact public facade selected by the scenario.
    pub api: ConfigResourceListingApi,
    /// Complete public operation bound.
    pub timeout_ms: u64,
}

/// One configuration-resource identity returned by the public API.
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdminConfigResource {
    /// Exact signed Kafka resource-type code.
    pub resource_type: i8,
    /// Exact Kafka resource name.
    pub name: String,
}

/// Public result for one configuration-resource listing.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdminConfigResourcesListing {
    /// Stable identity for the complete public call.
    pub operation_id: OperationId,
    /// Nonnegative Kafka throttle observation in milliseconds.
    pub throttle_time_ms: u64,
    /// Canonical resource identities returned by Kafka.
    pub resources: Vec<AdminConfigResource>,
}

/// Independently listed configuration-resource identities.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BrokerConfigResourcesState {
    /// Monotonic observation ordinal from the environment.
    pub observation: u64,
    /// Stable identity for the public call being corroborated.
    pub operation_id: OperationId,
    /// Canonical exact resource identities returned by Kafka's pinned CLI.
    pub resources: Vec<AdminConfigResource>,
}

#[cfg(test)]
#[path = "admin_config_resources_test.rs"]
mod test;
