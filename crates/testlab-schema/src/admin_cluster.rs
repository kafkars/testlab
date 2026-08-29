//! Cluster-admin payloads keep topology expectations outside adapter commands.

use serde::{Deserialize, Serialize};

use crate::{ClientId, OperationId};

/// Scenario intent for one bounded cluster description.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DescribeClusterAction {
    /// Existing client whose admin handle is used.
    pub client_id: ClientId,
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Complete public operation bound.
    pub timeout_ms: u64,
}

/// Wire payload for one bounded cluster description.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DescribeClusterCommand {
    /// Existing client whose admin handle is used.
    pub client_id: ClientId,
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Complete public operation bound.
    pub timeout_ms: u64,
}

/// Public cluster identity and broker set exposed by the packaged client.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdminClusterDescription {
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Cluster identity reported by the adapter, when available.
    pub cluster_id: Option<String>,
    /// Sorted broker identifiers reported by the adapter.
    pub broker_ids: Vec<i32>,
}
