//! Share-group Admin actions translate without leaking verifier expectations.

use testlab_schema::{
    AdapterCommand, AlterShareGroupOffsetsCommand, DeleteShareGroupOffsetsCommand,
    DeleteShareGroupsCommand, DescribeShareGroupCommand, DescribeShareGroupsCommand,
    ListShareGroupOffsetsCommand, ScenarioAction,
};

use crate::runner_protocol::ExpectedEvent;

pub(crate) fn translate(action: &ScenarioAction) -> Option<(AdapterCommand, ExpectedEvent)> {
    Some(match action {
        ScenarioAction::DescribeShareGroup(value) => (
            AdapterCommand::DescribeShareGroup(DescribeShareGroupCommand {
                client_id: value.client_id.clone(),
                operation_id: value.operation_id.clone(),
                group_id: value.group_id.clone(),
                timeout_ms: value.timeout_ms,
            }),
            ExpectedEvent::ShareGroupDescribed {
                operation_id: value.operation_id.clone(),
                group_id: value.group_id.clone(),
            },
        ),
        ScenarioAction::DescribeShareGroups(value) => (
            AdapterCommand::DescribeShareGroups(DescribeShareGroupsCommand {
                client_id: value.client_id.clone(),
                operation_id: value.operation_id.clone(),
                group_ids: value
                    .groups
                    .iter()
                    .map(|group| group.group_id.clone())
                    .collect(),
                timeout_ms: value.timeout_ms,
            }),
            ExpectedEvent::ShareGroupsDescribed {
                operation_id: value.operation_id.clone(),
                group_ids: value
                    .groups
                    .iter()
                    .map(|group| group.group_id.clone())
                    .collect(),
            },
        ),
        ScenarioAction::ListShareGroupOffsets(value) => (
            AdapterCommand::ListShareGroupOffsets(ListShareGroupOffsetsCommand {
                client_id: value.client_id.clone(),
                operation_id: value.operation_id.clone(),
                group_id: value.group_id.clone(),
                topic: value.topic.clone(),
                partition: value.partition,
                timeout_ms: value.timeout_ms,
            }),
            ExpectedEvent::ShareGroupOffsetsListed {
                operation_id: value.operation_id.clone(),
                group_id: value.group_id.clone(),
                topic: value.topic.clone(),
                partition: value.partition,
            },
        ),
        ScenarioAction::AlterShareGroupOffsets(value) => (
            AdapterCommand::AlterShareGroupOffsets(AlterShareGroupOffsetsCommand {
                client_id: value.client_id.clone(),
                operation_id: value.operation_id.clone(),
                group_id: value.group_id.clone(),
                topic: value.topic.clone(),
                partition: value.partition,
                start_offset: value.start_offset,
                timeout_ms: value.timeout_ms,
            }),
            ExpectedEvent::ShareGroupOffsetsAltered {
                operation_id: value.operation_id.clone(),
                group_id: value.group_id.clone(),
                topic: value.topic.clone(),
                partition: value.partition,
            },
        ),
        ScenarioAction::DeleteShareGroupOffsets(value) => (
            AdapterCommand::DeleteShareGroupOffsets(DeleteShareGroupOffsetsCommand {
                client_id: value.client_id.clone(),
                operation_id: value.operation_id.clone(),
                group_id: value.group_id.clone(),
                topic: value.topic.clone(),
                timeout_ms: value.timeout_ms,
            }),
            ExpectedEvent::ShareGroupOffsetsDeleted {
                operation_id: value.operation_id.clone(),
                group_id: value.group_id.clone(),
                topic: value.topic.clone(),
            },
        ),
        ScenarioAction::DeleteShareGroups(value) => (
            AdapterCommand::DeleteShareGroups(DeleteShareGroupsCommand {
                client_id: value.client_id.clone(),
                operation_id: value.operation_id.clone(),
                group_ids: value.group_ids.clone(),
                timeout_ms: value.timeout_ms,
            }),
            ExpectedEvent::ShareGroupsDeleted {
                operation_id: value.operation_id.clone(),
                group_ids: value.group_ids.clone(),
            },
        ),
        _ => return None,
    })
}
