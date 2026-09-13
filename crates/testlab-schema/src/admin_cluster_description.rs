//! Cluster-description payloads retain options and fenced-broker facts.

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
    /// Whether fenced brokers enter the public result.
    pub include_fenced_brokers: bool,
    /// Whether Kafka must return the cluster authorization bitfield.
    pub include_authorized_operations: bool,
    /// Known fenced broker IDs that remain private to the scenario.
    pub expected_fenced_broker_ids: Vec<i32>,
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
    /// Whether fenced brokers enter the public result.
    pub include_fenced_brokers: bool,
    /// Whether Kafka must return the cluster authorization bitfield.
    pub include_authorized_operations: bool,
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
    /// Sorted identifiers of every broker returned by the public API.
    pub broker_ids: Vec<i32>,
    /// Sorted broker IDs whose public descriptions carry the fenced marker.
    pub fenced_broker_ids: Vec<i32>,
    /// Raw Kafka authorization bitfield, when requested.
    pub authorized_operations: Option<i32>,
}
