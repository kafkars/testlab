//! Generic configuration-resource listing keeps scenario expectations off the wire.

use serde::{Deserialize, Serialize};

use crate::{ClientId, OperationId};

/// Scenario intent for one bounded topic-resource listing.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ListConfigResourcesAction {
    /// Existing client whose admin handle is used.
    pub client_id: ClientId,
    /// Stable identity for the complete public call.
    pub operation_id: OperationId,
    /// Exact topic resources required in the public result and independent metadata.
    pub required_topics: Vec<String>,
    /// Complete public operation bound.
    pub timeout_ms: u64,
}

/// Wire payload for one bounded topic-resource listing.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ListConfigResourcesCommand {
    /// Existing client whose admin handle is used.
    pub client_id: ClientId,
    /// Stable identity for the complete public call.
    pub operation_id: OperationId,
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

/// Public result for one filtered configuration-resource listing.
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

#[cfg(test)]
#[path = "admin_config_resources_test.rs"]
mod test;
