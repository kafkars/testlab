//! Admin observer targets exist only for an exact scenario-action and wire-command pair.

use std::collections::BTreeSet;

use testlab_schema::{AdapterCommand, OperationId, ScenarioAction};

use crate::observer_admin_batch_topic_target;
use crate::observer_admin_config_target;
use crate::observer_admin_group_target;
use crate::observer_admin_partition_offsets_target;
use crate::observer_admin_plural_group_target;
use crate::observer_admin_topic_target;
use crate::observer_error::ObserverError;

pub(super) type TargetMatch = (AdapterCommand, AdminTarget);

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum AdminTarget {
    Topic(TopicTarget),
    Topics(ListTarget),
    Cluster(OperationId),
    ConsumerGroups(ListTarget),
    ConsumerGroup(GroupTarget),
    ConsumerGroupOffset(OffsetTarget),
    ConsumerGroupOffsets(GroupOffsetsTarget),
    ConsumerGroupsOffsets(GroupsOffsetsTarget),
    ClassicGroups(ClassicGroupsTarget),
    TopicConfig(ConfigTarget),
    PartitionOffsets(PartitionOffsetsTarget),
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
pub(super) struct ConfigTarget {
    pub(super) operation_id: OperationId,
    pub(super) topic: String,
    pub(super) config_name: String,
    pub(super) expected_value: String,
    pub(super) poll_expected: bool,
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

impl AdminTarget {
    pub(super) fn from_exact(
        action: &ScenarioAction,
        command: &AdapterCommand,
    ) -> Result<Option<Self>, ObserverError> {
        let matched = match observer_admin_batch_topic_target::match_action(action)?
            .or(observer_admin_topic_target::match_action(action)?)
            .or_else(|| observer_admin_partition_offsets_target::match_action(action))
            .or(observer_admin_config_target::match_action(action)?)
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
            Self::Topic(target) => &target.operation_id,
            Self::Topics(target) | Self::ConsumerGroups(target) => &target.operation_id,
            Self::Cluster(operation_id) => operation_id,
            Self::ConsumerGroup(target) => &target.operation_id,
            Self::ConsumerGroupOffset(target) => &target.operation_id,
            Self::ConsumerGroupOffsets(target) => &target.operation_id,
            Self::ConsumerGroupsOffsets(target) => &target.operation_id,
            Self::ClassicGroups(target) => &target.operation_id,
            Self::TopicConfig(target) => &target.operation_id,
            Self::PartitionOffsets(target) => &target.operation_id,
        }
    }

    pub(super) fn observation_count(&self) -> usize {
        match self {
            Self::Topics(target) | Self::ConsumerGroups(target) => target.names.len(),
            Self::ConsumerGroupOffsets(target) => target.offsets.len(),
            Self::ConsumerGroupsOffsets(target) => {
                target.groups.iter().map(|group| group.offsets.len()).sum()
            }
            Self::ClassicGroups(target) => target.group_ids.len(),
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

pub(super) fn unique<T: Ord>(
    values: &[T],
    operation_id: &OperationId,
    resource: &str,
) -> Result<(), ObserverError> {
    let mut seen = BTreeSet::new();
    if values.iter().any(|value| !seen.insert(value)) {
        return Err(invalid(
            operation_id,
            format!("contains duplicate {resource}"),
        ));
    }
    Ok(())
}

pub(super) fn invalid(operation_id: &OperationId, detail: impl std::fmt::Display) -> ObserverError {
    ObserverError::InvalidTarget(format!("admin operation {operation_id} {detail}"))
}

pub(super) fn ordinal(first: u64, index: usize) -> Result<u64, ObserverError> {
    first
        .checked_add(u64::try_from(index).map_err(|_| ObserverError::ObservationOverflow)?)
        .ok_or(ObserverError::ObservationOverflow)
}
