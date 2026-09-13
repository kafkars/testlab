//! Partition-reassignment payloads retain exact caller order and broker state.

use serde::{Deserialize, Serialize};

use crate::{ClientId, OperationId};

/// One exact topic-partition selection.
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PartitionReassignmentSelection {
    /// Exact topic name.
    pub topic: String,
    /// Exact nonnegative partition.
    pub partition: i32,
}

/// One requested exact replacement replica assignment.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PartitionReassignmentChangeSpec {
    /// Exact topic name.
    pub topic: String,
    /// Exact nonnegative partition.
    pub partition: i32,
    /// Ordered unique nonnegative replacement broker IDs.
    pub replicas: Vec<i32>,
}

/// Scenario intent for one caller-ordered partition-reassignment mutation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AlterPartitionReassignmentsAction {
    /// Existing client whose admin handle is used.
    pub client_id: ClientId,
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Caller-ordered exact replacement assignments.
    pub changes: Vec<PartitionReassignmentChangeSpec>,
    /// Whether Kafka may change each partition's replication factor.
    pub allow_replication_factor_change: bool,
    /// Complete public operation bound.
    pub timeout_ms: u64,
}

/// Wire payload for one caller-ordered partition-reassignment mutation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AlterPartitionReassignmentsCommand {
    /// Existing client whose admin handle is used.
    pub client_id: ClientId,
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Caller-ordered exact replacement assignments.
    pub changes: Vec<PartitionReassignmentChangeSpec>,
    /// Whether Kafka may change each partition's replication factor.
    pub allow_replication_factor_change: bool,
    /// Complete public operation bound.
    pub timeout_ms: u64,
}

/// One caller-positioned public mutation outcome.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdminPartitionReassignmentOutcome {
    /// Exact requested topic.
    pub topic: String,
    /// Exact requested partition.
    pub partition: i32,
    /// Stable normalized public error, or none on success.
    pub error_code: Option<String>,
}

/// Public completion for one partition-reassignment mutation batch.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdminPartitionReassignmentsAlteration {
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Maximum nonnegative throttle observed across broker calls.
    pub throttle_time_ms: u64,
    /// One outcome per request in original caller order.
    pub outcomes: Vec<AdminPartitionReassignmentOutcome>,
}

/// Exact active reassignment fields shared by public and independent evidence.
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PartitionReassignmentSnapshot {
    /// Exact topic name.
    pub topic: String,
    /// Exact nonnegative partition.
    pub partition: i32,
    /// Kafka's ordered current replica IDs.
    pub replicas: Vec<i32>,
    /// Kafka's ordered adding-replica IDs.
    pub adding_replicas: Vec<i32>,
    /// Kafka's ordered removing-replica IDs.
    pub removing_replicas: Vec<i32>,
}

/// Scenario intent for selected or all active partition reassignments.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ListPartitionReassignmentsAction {
    /// Existing client whose admin handle is used.
    pub client_id: ClientId,
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Caller-ordered selected partitions, or none for all active rows.
    pub targets: Option<Vec<PartitionReassignmentSelection>>,
    /// Exact expected active rows in public result order.
    pub expected_reassignments: Vec<PartitionReassignmentSnapshot>,
    /// Complete public operation bound.
    pub timeout_ms: u64,
}

/// Wire payload for selected or all active partition reassignments.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ListPartitionReassignmentsCommand {
    /// Existing client whose admin handle is used.
    pub client_id: ClientId,
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Caller-ordered selected partitions, or none for all active rows.
    pub targets: Option<Vec<PartitionReassignmentSelection>>,
    /// Complete public operation bound.
    pub timeout_ms: u64,
}

/// Public result for selected or all active partition reassignments.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdminPartitionReassignmentsListing {
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Nonnegative broker throttle observation.
    pub throttle_time_ms: u64,
    /// Deterministically ordered active rows.
    pub reassignments: Vec<PartitionReassignmentSnapshot>,
}

/// One independently observed converged partition assignment.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BrokerPartitionAssignment {
    /// Exact topic name.
    pub topic: String,
    /// Exact nonnegative partition.
    pub partition: i32,
    /// Exact nonnegative leader broker ID.
    pub leader_id: i32,
    /// Exact ordered replica broker IDs.
    pub replicas: Vec<i32>,
    /// Canonical ascending in-sync replica broker IDs.
    pub in_sync_replicas: Vec<i32>,
}

/// One complete independently observed topic partition set.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BrokerTopicPartitionSet {
    /// Exact topic name.
    pub topic: String,
    /// Canonical ascending broker-visible partition indices.
    pub partitions: Vec<i32>,
}

/// Independent metadata state after one replica-placement mutation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BrokerPartitionAssignmentsState {
    /// Monotonic observation ordinal from the environment.
    pub observation: u64,
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Complete canonical topic partition sets represented by the assignments.
    pub topic_partitions: Vec<BrokerTopicPartitionSet>,
    /// Assignments in original mutation request order.
    pub assignments: Vec<BrokerPartitionAssignment>,
}

/// Independent pinned-CLI active reassignment state.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BrokerPartitionReassignmentsState {
    /// Monotonic observation ordinal from the environment.
    pub observation: u64,
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Canonical topic-byte and partition ordered active rows.
    pub reassignments: Vec<PartitionReassignmentSnapshot>,
}

pub(crate) mod action_validation;
pub(crate) mod transition_validation;

#[cfg(test)]
#[path = "admin_partition_reassignments_test.rs"]
mod tests;
