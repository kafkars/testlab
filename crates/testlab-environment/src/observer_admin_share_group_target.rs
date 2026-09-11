//! Share-group observation targets require an exact action and wire command pair.

use testlab_schema::{
    AdapterCommand, AlterShareGroupOffsetsCommand, DeleteShareGroupOffsetsCommand,
    DeleteShareGroupsCommand, DescribeShareGroupCommand, DescribeShareGroupsCommand,
    ListShareGroupOffsetsCommand, ListShareGroupsOffsetsCommand, ScenarioAction,
    ShareGroupOffsetSelection, ShareGroupOffsetsSelection,
};

use crate::observer_admin_target::{
    AdminTarget, ShareGroupOffsetSelectionTarget, ShareGroupOffsetTarget,
    ShareGroupOffsetsSelectionTarget, ShareGroupTarget, ShareGroupsOffsetsTarget,
    ShareGroupsTarget, TargetMatch, invalid, unique,
};
use crate::observer_error::ObserverError;

pub(super) fn match_action(action: &ScenarioAction) -> Result<Option<TargetMatch>, ObserverError> {
    Ok(Some(match action {
        ScenarioAction::DescribeShareGroup(action) => (
            AdapterCommand::DescribeShareGroup(DescribeShareGroupCommand {
                client_id: action.client_id.clone(),
                operation_id: action.operation_id.clone(),
                group_id: action.group_id.clone(),
                timeout_ms: action.timeout_ms,
            }),
            AdminTarget::ShareGroup(ShareGroupTarget {
                operation_id: action.operation_id.clone(),
                group_id: action.group_id.clone(),
            }),
        ),
        ScenarioAction::DescribeShareGroups(action) => {
            let group_ids = action
                .groups
                .iter()
                .map(|group| group.group_id.clone())
                .collect::<Vec<_>>();
            unique(&group_ids, &action.operation_id, "Share-group identity")?;
            (
                AdapterCommand::DescribeShareGroups(DescribeShareGroupsCommand {
                    client_id: action.client_id.clone(),
                    operation_id: action.operation_id.clone(),
                    group_ids: group_ids.clone(),
                    timeout_ms: action.timeout_ms,
                }),
                AdminTarget::ShareGroupDescriptions(ShareGroupsTarget {
                    operation_id: action.operation_id.clone(),
                    group_ids,
                }),
            )
        }
        ScenarioAction::ListShareGroupOffsets(action) => (
            AdapterCommand::ListShareGroupOffsets(ListShareGroupOffsetsCommand {
                client_id: action.client_id.clone(),
                operation_id: action.operation_id.clone(),
                group_id: action.group_id.clone(),
                topic: action.topic.clone(),
                partition: action.partition,
                timeout_ms: action.timeout_ms,
            }),
            AdminTarget::ShareGroupOffset(ShareGroupOffsetTarget {
                operation_id: action.operation_id.clone(),
                group_id: action.group_id.clone(),
                topic: action.topic.clone(),
                partition: action.partition,
            }),
        ),
        ScenarioAction::ListShareGroupsOffsets(action) => {
            let group_ids = action
                .groups
                .iter()
                .map(|group| group.group_id.clone())
                .collect::<Vec<_>>();
            unique(&group_ids, &action.operation_id, "Share-group identity")?;
            let mut groups = Vec::with_capacity(action.groups.len());
            let mut selections = Vec::with_capacity(action.groups.len());
            for group in &action.groups {
                let identities = group
                    .partitions
                    .iter()
                    .map(|partition| (partition.topic.as_str(), partition.partition))
                    .collect::<Vec<_>>();
                if identities.iter().enumerate().any(|(index, identity)| {
                    identities[..index].iter().any(|prior| prior == identity)
                }) {
                    return Err(invalid(
                        &action.operation_id,
                        "contains duplicate Share-group offset selections",
                    ));
                }
                selections.push(ShareGroupOffsetsSelection {
                    group_id: group.group_id.clone(),
                    partitions: group
                        .partitions
                        .iter()
                        .map(|partition| ShareGroupOffsetSelection {
                            topic: partition.topic.clone(),
                            partition: partition.partition,
                        })
                        .collect(),
                });
                groups.push(ShareGroupOffsetsSelectionTarget {
                    group_id: group.group_id.clone(),
                    offsets: group
                        .partitions
                        .iter()
                        .map(|partition| ShareGroupOffsetSelectionTarget {
                            topic: partition.topic.clone(),
                            partition: partition.partition,
                        })
                        .collect(),
                });
            }
            (
                AdapterCommand::ListShareGroupsOffsets(ListShareGroupsOffsetsCommand {
                    client_id: action.client_id.clone(),
                    operation_id: action.operation_id.clone(),
                    groups: selections,
                    timeout_ms: action.timeout_ms,
                }),
                AdminTarget::ShareGroupsOffsets(ShareGroupsOffsetsTarget {
                    operation_id: action.operation_id.clone(),
                    groups,
                }),
            )
        }
        ScenarioAction::AlterShareGroupOffsets(action) => (
            AdapterCommand::AlterShareGroupOffsets(AlterShareGroupOffsetsCommand {
                client_id: action.client_id.clone(),
                operation_id: action.operation_id.clone(),
                group_id: action.group_id.clone(),
                topic: action.topic.clone(),
                partition: action.partition,
                start_offset: action.start_offset,
                timeout_ms: action.timeout_ms,
            }),
            AdminTarget::ShareGroupOffset(ShareGroupOffsetTarget {
                operation_id: action.operation_id.clone(),
                group_id: action.group_id.clone(),
                topic: action.topic.clone(),
                partition: action.partition,
            }),
        ),
        ScenarioAction::DeleteShareGroupOffsets(action) => (
            AdapterCommand::DeleteShareGroupOffsets(DeleteShareGroupOffsetsCommand {
                client_id: action.client_id.clone(),
                operation_id: action.operation_id.clone(),
                group_id: action.group_id.clone(),
                topic: action.topic.clone(),
                timeout_ms: action.timeout_ms,
            }),
            AdminTarget::ShareGroupOffset(ShareGroupOffsetTarget {
                operation_id: action.operation_id.clone(),
                group_id: action.group_id.clone(),
                topic: action.topic.clone(),
                partition: action.partition,
            }),
        ),
        ScenarioAction::DeleteShareGroups(action) => {
            unique(
                &action.group_ids,
                &action.operation_id,
                "Share-group identity",
            )?;
            (
                AdapterCommand::DeleteShareGroups(DeleteShareGroupsCommand {
                    client_id: action.client_id.clone(),
                    operation_id: action.operation_id.clone(),
                    group_ids: action.group_ids.clone(),
                    timeout_ms: action.timeout_ms,
                }),
                AdminTarget::ShareGroups(ShareGroupsTarget {
                    operation_id: action.operation_id.clone(),
                    group_ids: action.group_ids.clone(),
                }),
            )
        }
        _ => return Ok(None),
    }))
}
