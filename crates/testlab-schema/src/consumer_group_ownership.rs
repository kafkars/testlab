//! Consumer ownership values separate scenario expectations from public client evidence.

use serde::{Deserialize, Serialize};

use crate::{ConsumedRecord, ConsumerId, GroupMembershipEpoch, OperationId};

/// One exact Kafka topic-partition identity.
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TopicPartitionIdentity {
    /// Exact topic spelling.
    pub topic: String,
    /// Zero-based partition index.
    pub partition: i32,
}

/// Replaces one direct consumer assignment with multiple partitions at their beginnings.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AssignBeginningBatchAction {
    /// Existing assigned consumer.
    pub consumer_id: ConsumerId,
    /// Ordered unique assignment.
    pub partitions: Vec<TopicPartitionIdentity>,
    /// Complete assignment bound.
    pub timeout_ms: u64,
}

/// Wire request for one multi-partition direct assignment.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AssignBeginningBatchCommand {
    /// Existing assigned consumer.
    pub consumer_id: ConsumerId,
    /// Ordered unique assignment.
    pub partitions: Vec<TopicPartitionIdentity>,
    /// Complete assignment bound.
    pub timeout_ms: u64,
}

/// Waits for a declared group member set to expose one stable complete assignment.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ObserveGroupAssignmentsAction {
    /// Stable observation identity.
    pub operation_id: OperationId,
    /// Ordered unique live consumers expected in the result.
    pub consumer_ids: Vec<ConsumerId>,
    /// Complete expected assignment retained only in the scenario.
    pub partitions: Vec<TopicPartitionIdentity>,
    /// Complete observation bound.
    pub timeout_ms: u64,
}

/// Wire request for stable public assignment snapshots.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ObserveGroupAssignmentsCommand {
    /// Stable observation identity.
    pub operation_id: OperationId,
    /// Ordered unique live consumers to observe.
    pub consumer_ids: Vec<ConsumerId>,
    /// Complete observation bound.
    pub timeout_ms: u64,
}

/// Stable public assignment snapshots and transitions observed while settling.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GroupAssignmentsObservation {
    /// Stable observation identity.
    pub operation_id: OperationId,
    /// Public transitions drained while reaching the stable snapshot.
    pub transitions: Vec<GroupAssignmentTransition>,
    /// Stable snapshots in caller consumer order.
    pub assignments: Vec<GroupConsumerAssignment>,
}

/// One public assignment transition kind.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum GroupAssignmentTransitionKind {
    /// A new assignment became current.
    Assigned,
    /// A current assignment entered graceful revocation.
    Revoking,
    /// An assignment was lost without graceful completion.
    Lost,
}

/// One public transition attributed to its adapter consumer identity.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GroupAssignmentTransition {
    /// Consumer that emitted the transition.
    pub consumer_id: ConsumerId,
    /// Transition kind.
    pub kind: GroupAssignmentTransitionKind,
    /// Nonreused local assignment fence.
    pub assignment_epoch: u64,
    /// Ordered unique transition partitions.
    pub partitions: Vec<TopicPartitionIdentity>,
}

/// One current public consumer assignment and matching membership metadata.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GroupConsumerAssignment {
    /// Adapter consumer identity.
    pub consumer_id: ConsumerId,
    /// Exact Kafka group identity.
    pub group_id: String,
    /// Broker-issued member identity.
    pub member_id: String,
    /// Broker-issued protocol-specific membership epoch.
    pub group_epoch: GroupMembershipEpoch,
    /// Nonreused local assignment fence.
    pub assignment_epoch: u64,
    /// Ordered unique current partitions.
    pub partitions: Vec<TopicPartitionIdentity>,
}

/// Receives and commits a complete expected record set across live group members.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GroupReceiveSetAction {
    /// Stable receive identity.
    pub receive_id: OperationId,
    /// Ordered unique live consumers eligible to receive.
    pub consumer_ids: Vec<ConsumerId>,
    /// Exact expected producer operations retained only in the scenario.
    pub expected_operation_ids: Vec<OperationId>,
    /// Complete receive and commit bound.
    pub timeout_ms: u64,
}

/// Wire request for a structural number of records across live group members.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GroupReceiveSetCommand {
    /// Stable receive identity.
    pub receive_id: OperationId,
    /// Ordered unique live consumers eligible to receive.
    pub consumer_ids: Vec<ConsumerId>,
    /// Structural number of records requested without expected identities.
    pub record_count: usize,
    /// Complete receive and commit bound.
    pub timeout_ms: u64,
}

/// One completed multi-member receive operation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GroupReceiveSetCompletion {
    /// Stable receive identity.
    pub receive_id: OperationId,
    /// Public results in caller consumer order.
    pub members: Vec<GroupReceiveMemberCompletion>,
}

/// Records and commit result attributed to one group member.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GroupReceiveMemberCompletion {
    /// Adapter consumer identity.
    pub consumer_id: ConsumerId,
    /// Exact records returned through the public API.
    pub records: Vec<ConsumedRecord>,
    /// Whether every retained batch checkpoint committed.
    pub committed: bool,
    /// Public group metadata observed after receiving.
    pub group_epoch: Option<GroupMembershipEpoch>,
}
