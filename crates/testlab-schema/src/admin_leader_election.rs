//! Leader-election payloads retain scope, policy, caller order, and broker truth.

use serde::{Deserialize, Serialize};

use crate::{ClientId, OperationId};

/// Public Kafka leader-election policy.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AdminLeaderElectionType {
    /// Elect the first eligible replica in the assignment.
    Preferred,
    /// Permit election of an out-of-sync replica.
    Unclean,
}

/// One exact topic-partition selected for leader election.
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LeaderElectionSelection {
    /// Exact topic name.
    pub topic: String,
    /// Exact nonnegative partition.
    pub partition: i32,
}

/// Scenario intent for selected or cluster-wide public leader election.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ElectLeadersAction {
    /// Existing client whose admin handle is used.
    pub client_id: ClientId,
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Explicit election policy.
    pub election_type: AdminLeaderElectionType,
    /// Caller-ordered selected partitions, or none for all partitions.
    pub targets: Option<Vec<LeaderElectionSelection>>,
    /// Scenario-only partitions that cluster-wide results must contain.
    #[serde(default)]
    pub required_partitions: Vec<LeaderElectionSelection>,
    /// Whether evidence must prove a distinct pre-election leader.
    #[serde(default)]
    pub require_leader_change: bool,
    /// Complete public operation bound.
    pub timeout_ms: u64,
}

/// Wire payload for selected or cluster-wide public leader election.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ElectLeadersCommand {
    /// Existing client whose admin handle is used.
    pub client_id: ClientId,
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Explicit election policy.
    pub election_type: AdminLeaderElectionType,
    /// Caller-ordered selected partitions, or none for all partitions.
    pub targets: Option<Vec<LeaderElectionSelection>>,
    /// Complete public operation bound.
    pub timeout_ms: u64,
}

/// One deterministic public leader-election outcome.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdminLeaderElectionOutcome {
    /// Exact returned topic.
    pub topic: String,
    /// Exact returned partition.
    pub partition: i32,
    /// Stable normalized public error, or none on success.
    pub error_code: Option<String>,
}

/// Public completion for selected or cluster-wide leader election.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdminLeaderElection {
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Explicit election policy used by the public call.
    pub election_type: AdminLeaderElectionType,
    /// Maximum nonnegative throttle observed across broker calls.
    pub throttle_time_ms: u64,
    /// Caller-ordered selected outcomes or canonical cluster-wide outcomes.
    pub outcomes: Vec<AdminLeaderElectionOutcome>,
}

/// One independently observed partition after public leader election.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BrokerLeaderElectionPartition {
    /// Exact topic name.
    pub topic: String,
    /// Exact nonnegative partition.
    pub partition: i32,
    /// Exact nonnegative leader broker ID.
    pub leader_id: i32,
    /// Kafka's ordered replica broker IDs.
    pub replicas: Vec<i32>,
    /// Canonical ascending in-sync replica broker IDs.
    pub in_sync_replicas: Vec<i32>,
}

/// Independent metadata state after one public leader-election completion.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BrokerLeaderElectionState {
    /// Monotonic observation ordinal from the environment.
    pub observation: u64,
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Election policy whose postcondition was observed.
    pub election_type: AdminLeaderElectionType,
    /// Required partitions in scenario-selected order.
    pub partitions: Vec<BrokerLeaderElectionPartition>,
}

pub(crate) mod action_validation;
pub(crate) mod transition_validation;

#[cfg(test)]
#[path = "admin_leader_election_test.rs"]
mod tests;
