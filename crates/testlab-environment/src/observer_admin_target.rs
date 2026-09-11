use testlab_schema::{AdapterCommand, OperationId, ScenarioAction};

use crate::observer_admin_acl_target;
use crate::observer_admin_batch_topic_target;
use crate::observer_admin_client_quota_target;
use crate::observer_admin_config_target;
pub(super) use crate::observer_admin_config_types::{ConfigBatchTarget, ConfigTarget};
use crate::observer_admin_consumer_group_deletion_batch_target;
use crate::observer_admin_group_target;
use crate::observer_admin_leader_election_target;
use crate::observer_admin_log_dirs_target;
use crate::observer_admin_offset_batch_target;
use crate::observer_admin_partition_offsets_target;
use crate::observer_admin_partition_reassignment_target;
use crate::observer_admin_plural_group_target;
use crate::observer_admin_producer_target;
pub(super) use crate::observer_admin_share_group_offset_batch_target::{
    ShareGroupOffsetSelectionTarget, ShareGroupOffsetsSelectionTarget, ShareGroupsOffsetsTarget,
};
use crate::observer_admin_share_group_target;
pub(super) use crate::observer_admin_target_support::{invalid, ordinal, unique};
use crate::observer_admin_topic_deletion_batch_target;
use crate::observer_admin_topic_description_batch_target;
use crate::observer_admin_topic_target;
use crate::observer_admin_transaction_target;
use crate::observer_admin_user_scram_target;
use crate::observer_error::ObserverError;

pub(super) type TargetMatch = (AdapterCommand, AdminTarget);
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum AdminTarget {
    Acls(AclsTarget),
    ClientQuota(ClientQuotaTarget),
    UserScramCredential(UserScramCredentialTarget),
    Topic(TopicTarget),
    Topics(ListTarget),
    TopicIdentities(ListTarget),
    TopicDeletions(ListTarget),
    Cluster(OperationId),
    Features(OperationId),
    MetadataQuorum(OperationId),
    Producers(observer_admin_producer_target::ProducerTarget),
    LogDirs(observer_admin_log_dirs_target::LogDirsTarget),
    ReplicaLogDirs(observer_admin_log_dirs_target::LogDirsTarget),
    Transactions(observer_admin_transaction_target::TransactionTarget),
    ConsumerGroups(ListTarget),
    ConsumerGroupDeletions(ListTarget),
    ConsumerGroup(GroupTarget),
    ShareGroup(ShareGroupTarget),
    ShareGroupDescriptions(ShareGroupsTarget),
    ShareGroups(ShareGroupsTarget),
    ShareGroupOffset(ShareGroupOffsetTarget),
    ShareGroupsOffsets(ShareGroupsOffsetsTarget),
    ConsumerGroupOffset(OffsetTarget),
    ConsumerGroupOffsets(GroupOffsetsTarget),
    ConsumerGroupsOffsets(GroupsOffsetsTarget),
    ClassicGroups(ClassicGroupsTarget),
    TopicConfig(ConfigTarget),
    TopicConfigs(ConfigBatchTarget),
    PartitionOffsets(PartitionOffsetsTarget),
    PartitionOffsetsBatch(PartitionOffsetsBatchTarget),
    LeaderElection(observer_admin_leader_election_target::LeaderElectionTarget),
    PartitionAssignments(observer_admin_partition_reassignment_target::PartitionAssignmentsTarget),
    PartitionReassignments(
        observer_admin_partition_reassignment_target::PartitionReassignmentsTarget,
    ),
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct AclsTarget {
    pub(super) operation_id: OperationId,
    pub(super) bindings: Vec<testlab_schema::LiteralAclBinding>,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ClientQuotaTarget {
    pub(super) operation_id: OperationId,
    pub(super) user: String,
    pub(super) direction: testlab_schema::BrokerQuotaDirection,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct UserScramCredentialTarget {
    pub(super) operation_id: OperationId,
    pub(super) user: String,
    pub(super) mechanism: testlab_schema::ScramCredentialMechanism,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct TopicTarget {
    pub(super) operation_id: OperationId,
    pub(super) topic: String,
    pub(super) expected_partitions: Option<Vec<i32>>,
    pub(super) expected_exists: bool,
    pub(super) poll_expected: bool,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ListTarget {
    pub(super) operation_id: OperationId,
    pub(super) names: Vec<String>,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct GroupTarget {
    pub(super) operation_id: OperationId,
    pub(super) group_id: String,
    pub(super) expected_member_count: Option<u32>,
    pub(super) expected_exists: bool,
    pub(super) poll_expected: bool,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ShareGroupTarget {
    pub(super) operation_id: OperationId,
    pub(super) group_id: String,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ShareGroupsTarget {
    pub(super) operation_id: OperationId,
    pub(super) group_ids: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ShareGroupOffsetTarget {
    pub(super) operation_id: OperationId,
    pub(super) group_id: String,
    pub(super) topic: String,
    pub(super) partition: i32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct OffsetTarget {
    pub(super) operation_id: OperationId,
    pub(super) group_id: String,
    pub(super) topic: String,
    pub(super) partition: i32,
    pub(super) expected_offset: Option<i64>,
    pub(super) poll_expected: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct GroupOffsetTarget {
    pub(super) topic: String,
    pub(super) partition: i32,
    pub(super) expected_offset: Option<i64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct GroupOffsetsSelectionTarget {
    pub(super) group_id: String,
    pub(super) offsets: Vec<GroupOffsetTarget>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct GroupOffsetsTarget {
    pub(super) operation_id: OperationId,
    pub(super) group_id: String,
    pub(super) offsets: Vec<GroupOffsetTarget>,
    pub(super) poll_expected: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct GroupsOffsetsTarget {
    pub(super) operation_id: OperationId,
    pub(super) groups: Vec<GroupOffsetsSelectionTarget>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ClassicGroupsTarget {
    pub(super) operation_id: OperationId,
    pub(super) group_ids: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct PartitionOffsetsTarget {
    pub(super) operation_id: OperationId,
    pub(super) topic: String,
    pub(super) partition: i32,
    pub(super) expected_low: Option<i64>,
    pub(super) expected_high: Option<i64>,
    pub(super) poll_expected: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct PartitionOffsetsBatchTarget {
    pub(super) operation_id: OperationId,
    pub(super) offsets: Vec<PartitionOffsetsTarget>,
}

impl AdminTarget {
    pub(super) fn from_exact(
        action: &ScenarioAction,
        command: &AdapterCommand,
    ) -> Result<Option<Self>, ObserverError> {
        let matched = match observer_admin_user_scram_target::match_action(action)?
            .or(observer_admin_share_group_target::match_action(action)?)
            .or(observer_admin_client_quota_target::match_action(action)?)
            .or(observer_admin_acl_target::match_action(action)?)
            .or(observer_admin_offset_batch_target::match_action(action)?)
            .or(observer_admin_batch_topic_target::match_action(action)?)
            .or(observer_admin_topic_deletion_batch_target::match_action(
                action,
            )?)
            .or(observer_admin_topic_description_batch_target::match_action(
                action,
            )?)
            .or(observer_admin_topic_target::match_action(action)?)
            .or_else(|| observer_admin_partition_offsets_target::match_action(action))
            .or_else(|| observer_admin_leader_election_target::match_action(action))
            .or_else(|| observer_admin_partition_reassignment_target::match_action(action))
            .or(observer_admin_config_target::match_action(action)?)
            .or(observer_admin_producer_target::match_action(action)?)
            .or(observer_admin_log_dirs_target::match_action(action)?)
            .or(observer_admin_transaction_target::match_action(action)?)
            .or(observer_admin_consumer_group_deletion_batch_target::match_action(action)?)
        {
            Some(matched) => Some(matched),
            None => observer_admin_plural_group_target::match_action(action)?
                .or(observer_admin_group_target::match_action(action)?),
        };
        let Some((expected, target)) = matched else {
            return Ok(None);
        };
        if command != &expected {
            return Err(invalid(
                target.operation_id(),
                "wire command does not exactly match the scenario action",
            ));
        }
        Ok(Some(target))
    }

    pub(super) fn operation_id(&self) -> &OperationId {
        match self {
            Self::Acls(target) => &target.operation_id,
            Self::ClientQuota(target) => &target.operation_id,
            Self::UserScramCredential(target) => &target.operation_id,
            Self::Topic(target) => &target.operation_id,
            Self::Topics(target)
            | Self::TopicIdentities(target)
            | Self::TopicDeletions(target)
            | Self::ConsumerGroups(target)
            | Self::ConsumerGroupDeletions(target) => &target.operation_id,
            Self::Cluster(operation_id) => operation_id,
            Self::Features(operation_id) => operation_id,
            Self::MetadataQuorum(operation_id) => operation_id,
            Self::Producers(target) => &target.operation_id,
            Self::LogDirs(target) => &target.operation_id,
            Self::ReplicaLogDirs(target) => &target.operation_id,
            Self::Transactions(target) => target.operation_id(),
            Self::ConsumerGroup(target) => &target.operation_id,
            Self::ShareGroup(target) => &target.operation_id,
            Self::ShareGroupDescriptions(target) => &target.operation_id,
            Self::ShareGroups(target) => &target.operation_id,
            Self::ShareGroupOffset(target) => &target.operation_id,
            Self::ShareGroupsOffsets(target) => &target.operation_id,
            Self::ConsumerGroupOffset(target) => &target.operation_id,
            Self::ConsumerGroupOffsets(target) => &target.operation_id,
            Self::ConsumerGroupsOffsets(target) => &target.operation_id,
            Self::ClassicGroups(target) => &target.operation_id,
            Self::TopicConfig(target) => &target.operation_id,
            Self::TopicConfigs(target) => &target.operation_id,
            Self::PartitionOffsets(target) => &target.operation_id,
            Self::PartitionOffsetsBatch(target) => &target.operation_id,
            Self::LeaderElection(target) => &target.operation_id,
            Self::PartitionAssignments(target) => &target.operation_id,
            Self::PartitionReassignments(target) => &target.operation_id,
        }
    }

    pub(super) fn observation_count(&self) -> usize {
        match self {
            Self::Acls(target) => target.bindings.len(),
            Self::ClientQuota(_) => 1,
            Self::UserScramCredential(_) => 1,
            Self::Topics(target)
            | Self::TopicIdentities(target)
            | Self::TopicDeletions(target)
            | Self::ConsumerGroups(target)
            | Self::ConsumerGroupDeletions(target) => target.names.len(),
            Self::ConsumerGroupOffsets(target) => target.offsets.len(),
            Self::ConsumerGroupsOffsets(target) => {
                target.groups.iter().map(|group| group.offsets.len()).sum()
            }
            Self::ClassicGroups(target) => target.group_ids.len(),
            Self::ShareGroupDescriptions(target) => target.group_ids.len(),
            Self::ShareGroups(target) => target.group_ids.len(),
            Self::ShareGroupsOffsets(target) => {
                target.groups.iter().map(|group| group.offsets.len()).sum()
            }
            Self::TopicConfigs(target) => target.configs.len(),
            Self::PartitionOffsetsBatch(target) => target.offsets.len(),
            Self::Transactions(target) => target.observation_count(),
            _ => 1,
        }
    }

    pub(super) fn args(&self) -> Vec<String> {
        vec![
            "--operation-id".to_owned(),
            self.operation_id().to_string(),
            "--state-count".to_owned(),
            self.observation_count().to_string(),
        ]
    }
}
