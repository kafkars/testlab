//! Plural group-admin actions become exact ordered independent-observation targets.

use std::collections::BTreeSet;

use testlab_schema::{
    AdapterCommand, AlterConsumerGroupOffsetsAction, AlterConsumerGroupOffsetsCommand,
    ConsumerGroupOffsetSelection, ConsumerGroupOffsetsSelection, DeleteConsumerGroupOffsetsAction,
    DeleteConsumerGroupOffsetsCommand, DescribeClassicGroupsAction, DescribeClassicGroupsCommand,
    ListConsumerGroupOffsetsBatchAction, ListConsumerGroupOffsetsBatchCommand,
    ListConsumerGroupsOffsetsAction, ListConsumerGroupsOffsetsCommand, OperationId, ScenarioAction,
};

use crate::observer_admin_target::{
    AdminTarget, ClassicGroupsTarget, GroupOffsetTarget, GroupOffsetsSelectionTarget,
    GroupOffsetsTarget, GroupsOffsetsTarget, TargetMatch, invalid,
};
use crate::observer_error::ObserverError;

pub(super) fn match_action(action: &ScenarioAction) -> Result<Option<TargetMatch>, ObserverError> {
    let matched = match action {
        ScenarioAction::ListConsumerGroupOffsetsBatch(action) => list_one_group(action)?,
        ScenarioAction::ListConsumerGroupsOffsets(action) => list_many_groups(action)?,
        ScenarioAction::AlterConsumerGroupOffsets(action) => alter_offsets(action)?,
        ScenarioAction::DeleteConsumerGroupOffsets(action) => delete_offsets(action)?,
        ScenarioAction::DescribeClassicGroups(action) => describe_classic_groups(action)?,
        _ => return Ok(None),
    };
    Ok(Some(matched))
}

fn list_one_group(
    action: &ListConsumerGroupOffsetsBatchAction,
) -> Result<TargetMatch, ObserverError> {
    unique_offsets(
        &action.operation_id,
        action
            .partitions
            .iter()
            .map(|item| (item.topic.as_str(), item.partition)),
    )?;
    let selections = action
        .partitions
        .iter()
        .map(|item| ConsumerGroupOffsetSelection {
            topic: item.topic.clone(),
            partition: item.partition,
        })
        .collect();
    let offsets = action
        .partitions
        .iter()
        .map(|item| GroupOffsetTarget {
            topic: item.topic.clone(),
            partition: item.partition,
            expected_offset: Some(item.expected_offset),
        })
        .collect();
    Ok((
        AdapterCommand::ListConsumerGroupOffsetsBatch(ListConsumerGroupOffsetsBatchCommand {
            client_id: action.client_id.clone(),
            operation_id: action.operation_id.clone(),
            group_id: action.group_id.clone(),
            require_stable: action.require_stable,
            partitions: selections,
            timeout_ms: action.timeout_ms,
        }),
        AdminTarget::ConsumerGroupOffsets(GroupOffsetsTarget {
            operation_id: action.operation_id.clone(),
            group_id: action.group_id.clone(),
            offsets,
            poll_expected: false,
        }),
    ))
}

fn list_many_groups(
    action: &ListConsumerGroupsOffsetsAction,
) -> Result<TargetMatch, ObserverError> {
    unique_groups(
        &action.operation_id,
        action.groups.iter().map(|group| group.group_id.as_str()),
    )?;
    for group in &action.groups {
        unique_offsets(
            &action.operation_id,
            group
                .partitions
                .iter()
                .map(|item| (item.topic.as_str(), item.partition)),
        )?;
    }
    let groups = action
        .groups
        .iter()
        .map(|group| ConsumerGroupOffsetsSelection {
            group_id: group.group_id.clone(),
            partitions: group
                .partitions
                .iter()
                .map(|item| ConsumerGroupOffsetSelection {
                    topic: item.topic.clone(),
                    partition: item.partition,
                })
                .collect(),
        })
        .collect();
    let targets = action
        .groups
        .iter()
        .map(|group| GroupOffsetsSelectionTarget {
            group_id: group.group_id.clone(),
            offsets: group
                .partitions
                .iter()
                .map(|item| GroupOffsetTarget {
                    topic: item.topic.clone(),
                    partition: item.partition,
                    expected_offset: Some(item.expected_offset),
                })
                .collect(),
        })
        .collect();
    Ok((
        AdapterCommand::ListConsumerGroupsOffsets(ListConsumerGroupsOffsetsCommand {
            client_id: action.client_id.clone(),
            operation_id: action.operation_id.clone(),
            require_stable: action.require_stable,
            groups,
            timeout_ms: action.timeout_ms,
        }),
        AdminTarget::ConsumerGroupsOffsets(GroupsOffsetsTarget {
            operation_id: action.operation_id.clone(),
            groups: targets,
        }),
    ))
}

fn alter_offsets(action: &AlterConsumerGroupOffsetsAction) -> Result<TargetMatch, ObserverError> {
    unique_offsets(
        &action.operation_id,
        action
            .offsets
            .iter()
            .map(|item| (item.topic.as_str(), item.partition)),
    )?;
    let offsets = action
        .offsets
        .iter()
        .map(|item| GroupOffsetTarget {
            topic: item.topic.clone(),
            partition: item.partition,
            expected_offset: Some(item.offset),
        })
        .collect();
    Ok((
        AdapterCommand::AlterConsumerGroupOffsets(AlterConsumerGroupOffsetsCommand {
            client_id: action.client_id.clone(),
            operation_id: action.operation_id.clone(),
            group_id: action.group_id.clone(),
            offsets: action.offsets.clone(),
            timeout_ms: action.timeout_ms,
        }),
        AdminTarget::ConsumerGroupOffsets(GroupOffsetsTarget {
            operation_id: action.operation_id.clone(),
            group_id: action.group_id.clone(),
            offsets,
            poll_expected: true,
        }),
    ))
}

fn delete_offsets(action: &DeleteConsumerGroupOffsetsAction) -> Result<TargetMatch, ObserverError> {
    unique_offsets(
        &action.operation_id,
        action
            .partitions
            .iter()
            .map(|item| (item.topic.as_str(), item.partition)),
    )?;
    let offsets = action
        .partitions
        .iter()
        .map(|item| GroupOffsetTarget {
            topic: item.topic.clone(),
            partition: item.partition,
            expected_offset: None,
        })
        .collect();
    Ok((
        AdapterCommand::DeleteConsumerGroupOffsets(DeleteConsumerGroupOffsetsCommand {
            client_id: action.client_id.clone(),
            operation_id: action.operation_id.clone(),
            group_id: action.group_id.clone(),
            partitions: action.partitions.clone(),
            timeout_ms: action.timeout_ms,
        }),
        AdminTarget::ConsumerGroupOffsets(GroupOffsetsTarget {
            operation_id: action.operation_id.clone(),
            group_id: action.group_id.clone(),
            offsets,
            poll_expected: true,
        }),
    ))
}

fn describe_classic_groups(
    action: &DescribeClassicGroupsAction,
) -> Result<TargetMatch, ObserverError> {
    unique_groups(
        &action.operation_id,
        action.groups.iter().map(|group| group.group_id.as_str()),
    )?;
    let group_ids = action
        .groups
        .iter()
        .map(|group| group.group_id.clone())
        .collect::<Vec<_>>();
    Ok((
        AdapterCommand::DescribeClassicGroups(DescribeClassicGroupsCommand {
            client_id: action.client_id.clone(),
            operation_id: action.operation_id.clone(),
            group_ids: group_ids.clone(),
            timeout_ms: action.timeout_ms,
        }),
        AdminTarget::ClassicGroups(ClassicGroupsTarget {
            operation_id: action.operation_id.clone(),
            group_ids,
        }),
    ))
}

fn unique_groups<'a>(
    operation_id: &OperationId,
    groups: impl IntoIterator<Item = &'a str>,
) -> Result<(), ObserverError> {
    let mut seen = BTreeSet::new();
    if groups.into_iter().any(|group| !seen.insert(group)) {
        return Err(invalid(operation_id, "contains duplicate consumer groups"));
    }
    Ok(())
}

fn unique_offsets<'a>(
    operation_id: &OperationId,
    offsets: impl IntoIterator<Item = (&'a str, i32)>,
) -> Result<(), ObserverError> {
    let mut seen = BTreeSet::new();
    if offsets.into_iter().any(|offset| !seen.insert(offset)) {
        return Err(invalid(
            operation_id,
            "contains duplicate consumer-group offset selections",
        ));
    }
    Ok(())
}
