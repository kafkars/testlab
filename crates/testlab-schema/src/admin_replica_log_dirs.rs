//! Selected-replica log-directory payloads preserve caller order and placement facts.

use serde::{Deserialize, Serialize};

use crate::{ClientId, OperationId};

/// Scenario intent for one bounded selected-replica log-directory description.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DescribeReplicaLogDirsAction {
    /// Existing client whose admin handle is used.
    pub client_id: ClientId,
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Exact topic whose broker replicas are selected.
    pub topic: String,
    /// Exact nonnegative partition whose broker replicas are selected.
    pub partition: i32,
    /// Expected number of current placements in the controlled fixture.
    pub expected_replica_count: usize,
    /// Complete public operation bound.
    pub timeout_ms: u64,
}

/// Wire payload for one bounded selected-replica log-directory description.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DescribeReplicaLogDirsCommand {
    /// Existing client whose admin handle is used.
    pub client_id: ClientId,
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Exact topic whose broker replicas are selected.
    pub topic: String,
    /// Exact nonnegative partition whose broker replicas are selected.
    pub partition: i32,
    /// Complete public operation bound.
    pub timeout_ms: u64,
}

/// One exact topic-partition replica identity.
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReplicaLogDirIdentity {
    /// Exact topic name.
    pub topic: String,
    /// Exact nonnegative partition index.
    pub partition: i32,
    /// Exact nonnegative broker identity.
    pub broker_id: i32,
}

/// One public current or future replica placement.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReplicaLogDirLocationState {
    /// Exact absolute broker log-directory path.
    pub path: String,
    /// Exact signed replica offset lag.
    pub offset_lag: i64,
}

/// One selected public replica result in original caller order.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdminReplicaLogDirDescription {
    /// Exact selected replica identity.
    pub replica: ReplicaLogDirIdentity,
    /// Current placement when the selected broker hosts the replica.
    pub current: Option<ReplicaLogDirLocationState>,
    /// Future replacement placement when one exists.
    pub future: Option<ReplicaLogDirLocationState>,
}

/// Public selected-replica log-directory result exposed by the packaged client.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdminReplicaLogDirsDescription {
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Exact selected topic.
    pub topic: String,
    /// Exact selected partition.
    pub partition: i32,
    /// Maximum nonnegative throttle observed across broker calls.
    pub throttle_time_ms: u64,
    /// One result per discovered broker in deliberately descending caller order.
    pub replicas: Vec<AdminReplicaLogDirDescription>,
}

#[cfg(test)]
#[path = "admin_replica_log_dirs_test.rs"]
mod tests;
