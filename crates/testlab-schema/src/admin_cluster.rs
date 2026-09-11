//! Cluster-admin payloads keep discovery expectations outside adapter commands.

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

/// Scenario intent for one bounded cluster feature description.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DescribeFeaturesAction {
    /// Existing client whose admin handle is used.
    pub client_id: ClientId,
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Expected public ZK-migration readiness for the controlled environment.
    pub expected_zk_migration_ready: bool,
    /// Complete public operation bound.
    pub timeout_ms: u64,
}

/// Wire payload for one bounded cluster feature description.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DescribeFeaturesCommand {
    /// Existing client whose admin handle is used.
    pub client_id: ClientId,
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Complete public operation bound.
    pub timeout_ms: u64,
}

/// One exact named Kafka feature range.
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FeatureVersionRange {
    /// Exact Kafka feature name.
    pub name: String,
    /// Lowest represented feature level.
    pub min_version_level: i16,
    /// Highest represented feature level.
    pub max_version_level: i16,
}

/// Public feature metadata exposed by the packaged client.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdminFeaturesDescription {
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Caller-visible broker-supported feature ranges in canonical name order.
    pub supported_features: Vec<FeatureVersionRange>,
    /// Whether the negotiated response includes minimum-level-zero features.
    pub supported_features_complete: bool,
    /// Cluster finalized-feature epoch, when exposed by Kafka.
    pub finalized_features_epoch: Option<i64>,
    /// Caller-visible finalized feature ranges in canonical name order.
    pub finalized_features: Vec<FeatureVersionRange>,
    /// Exact public ZK-migration readiness flag.
    pub zk_migration_ready: bool,
}

/// One feature row independently normalized from Kafka's pinned administration CLI.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BrokerFeatureState {
    /// Exact Kafka feature name.
    pub name: String,
    /// Lowest broker-supported feature level.
    pub supported_min_version_level: i16,
    /// Highest broker-supported feature level.
    pub supported_max_version_level: i16,
    /// Cluster finalized level printed for this supported feature.
    pub finalized_version_level: i16,
}

/// Independent feature metadata snapshot from Kafka's pinned CLI.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BrokerFeaturesState {
    /// Monotonic observation ordinal from the environment.
    pub observation: u64,
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Complete CLI feature rows in canonical name order.
    pub features: Vec<BrokerFeatureState>,
    /// One epoch shared by every CLI feature row, when available.
    pub finalized_features_epoch: Option<i64>,
}

/// Scenario intent for one bounded active-producer description.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DescribeProducersAction {
    /// Existing client whose admin handle is used.
    pub client_id: ClientId,
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Exact topic whose partition producer state is queried.
    pub topic: String,
    /// Exact nonnegative partition whose producer state is queried.
    pub partition: i32,
    /// Expected number of active producers in the controlled fixture.
    pub expected_producer_count: usize,
    /// Complete public operation bound.
    pub timeout_ms: u64,
}

/// Wire payload for one bounded active-producer description.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DescribeProducersCommand {
    /// Existing client whose admin handle is used.
    pub client_id: ClientId,
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Exact topic whose partition producer state is queried.
    pub topic: String,
    /// Exact nonnegative partition whose producer state is queried.
    pub partition: i32,
    /// Complete public operation bound.
    pub timeout_ms: u64,
}

/// Exact active-producer fields shared by separately sourced snapshots.
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProducerStateSnapshot {
    /// Exact nonnegative producer identity.
    pub producer_id: i64,
    /// Exact nonnegative producer epoch.
    pub producer_epoch: i32,
    /// Last sequence, retaining Kafka's `-1` sentinel.
    pub last_sequence: i32,
    /// Last timestamp, retaining Kafka's `-1` sentinel.
    pub last_timestamp: i64,
    /// Exact nonnegative coordinator epoch.
    pub coordinator_epoch: i32,
    /// Current transaction's first offset, when active.
    pub current_transaction_start_offset: Option<i64>,
}

/// Public active-producer result exposed by the packaged client.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdminProducersDescription {
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Exact described topic.
    pub topic: String,
    /// Exact described partition.
    pub partition: i32,
    /// Producer states in canonical producer-ID order.
    pub producers: Vec<ProducerStateSnapshot>,
}

/// Independent active-producer snapshot from Kafka's pinned CLI.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BrokerProducersState {
    /// Monotonic observation ordinal from the environment.
    pub observation: u64,
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Exact described topic.
    pub topic: String,
    /// Exact described partition.
    pub partition: i32,
    /// Producer states in canonical producer-ID order.
    pub producers: Vec<ProducerStateSnapshot>,
}

#[cfg(test)]
#[path = "admin_cluster_test.rs"]
mod tests;
