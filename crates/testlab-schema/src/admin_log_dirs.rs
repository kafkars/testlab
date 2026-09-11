//! Log-directory payloads separate public capacity facts from pinned CLI state.

use serde::{Deserialize, Serialize};

use crate::{ClientId, OperationId};

/// Scenario intent for one bounded selected-partition log-directory description.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DescribeLogDirsAction {
    /// Existing client whose admin handle is used.
    pub client_id: ClientId,
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Exact topic whose replica logs are selected.
    pub topic: String,
    /// Exact nonnegative partition whose replica logs are selected.
    pub partition: i32,
    /// Expected number of current replicas in the controlled fixture.
    pub expected_replica_count: usize,
    /// Complete public operation bound.
    pub timeout_ms: u64,
}

/// Wire payload for one bounded selected-partition log-directory description.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DescribeLogDirsCommand {
    /// Existing client whose admin handle is used.
    pub client_id: ClientId,
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Exact topic whose replica logs are selected.
    pub topic: String,
    /// Exact nonnegative partition whose replica logs are selected.
    pub partition: i32,
    /// Complete public operation bound.
    pub timeout_ms: u64,
}

/// Exact replica-log fields shared by public and independent snapshots.
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LogDirReplicaState {
    /// Exact topic name.
    pub topic: String,
    /// Exact nonnegative partition index.
    pub partition: i32,
    /// Exact nonnegative local log size in bytes.
    pub size_bytes: i64,
    /// Exact signed follower or future-log offset lag.
    pub offset_lag: i64,
    /// Whether this is a future replacement replica log.
    pub is_future: bool,
}

/// One successful public log-directory description.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdminLogDirDescription {
    /// Exact absolute broker log-directory path.
    pub path: String,
    /// Total volume bytes when represented by the negotiated response.
    pub total_bytes: Option<i64>,
    /// Usable volume bytes when represented by the negotiated response.
    pub usable_bytes: Option<i64>,
    /// Cordon state when represented by the negotiated response.
    pub is_cordoned: Option<bool>,
    /// Canonical selected replica logs in this directory.
    pub replicas: Vec<LogDirReplicaState>,
}

/// One public broker result in original caller order.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdminBrokerLogDirsDescription {
    /// Exact nonnegative broker identity.
    pub broker_id: i32,
    /// Successful log directories in canonical path order.
    pub log_dirs: Vec<AdminLogDirDescription>,
}

/// Public selected-partition log-directory result exposed by the packaged client.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdminLogDirsDescription {
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Exact selected topic.
    pub topic: String,
    /// Exact selected partition.
    pub partition: i32,
    /// Maximum nonnegative throttle observed across broker calls.
    pub throttle_time_ms: u64,
    /// Per-broker results in deliberately descending caller order.
    pub brokers: Vec<AdminBrokerLogDirsDescription>,
}

/// One independently observed log directory without public-only capacity fields.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BrokerLogDirState {
    /// Exact absolute broker log-directory path.
    pub path: String,
    /// Canonical selected replica logs in this directory.
    pub replicas: Vec<LogDirReplicaState>,
}

/// One independently observed broker in canonical broker-ID order.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BrokerLogDirsBrokerState {
    /// Exact nonnegative broker identity.
    pub broker_id: i32,
    /// Successful log directories in canonical path order.
    pub log_dirs: Vec<BrokerLogDirState>,
}

/// Independent selected-topic log-directory snapshot from Kafka's pinned CLI.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BrokerLogDirsState {
    /// Monotonic observation ordinal from the environment.
    pub observation: u64,
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Exact selected topic.
    pub topic: String,
    /// Exact selected partition.
    pub partition: i32,
    /// Brokers in canonical broker-ID order.
    pub brokers: Vec<BrokerLogDirsBrokerState>,
}

#[cfg(test)]
#[path = "admin_log_dirs_test.rs"]
mod tests;

#[path = "admin_log_dirs_transition_validation.rs"]
pub(crate) mod transition_validation;

#[path = "admin_replica_log_dirs.rs"]
pub(crate) mod replica;

#[path = "admin_replica_log_dirs_action_validation.rs"]
pub(crate) mod replica_action_validation;
