//! Batched group-admin translation removes verifier expectations from wire commands.

use testlab_schema::{
    AdapterCommand, ConsumerGroupOffsetSelection, ConsumerGroupOffsetsSelection,
    DescribeClassicGroupsCommand, ListConsumerGroupOffsetsBatchCommand,
    ListConsumerGroupsOffsetsCommand, ScenarioAction,
};

use crate::runner_protocol::ExpectedEvent;

pub(super) fn translate(action: &ScenarioAction) -> Option<(AdapterCommand, ExpectedEvent)> {
    Some(match action {
        ScenarioAction::ListConsumerGroupOffsetsBatch(action) => (
            AdapterCommand::ListConsumerGroupOffsetsBatch(ListConsumerGroupOffsetsBatchCommand {
                client_id: action.client_id.clone(),
                operation_id: action.operation_id.clone(),
                group_id: action.group_id.clone(),
                partitions: action
                    .partitions
                    .iter()
                    .map(|value| ConsumerGroupOffsetSelection {
                        topic: value.topic.clone(),
                        partition: value.partition,
                    })
                    .collect(),
                require_stable: action.require_stable,
                timeout_ms: action.timeout_ms,
            }),
            ExpectedEvent::ConsumerGroupOffsetsListed {
                operation_id: action.operation_id.clone(),
            },
        ),
        ScenarioAction::ListConsumerGroupsOffsets(action) => (
            AdapterCommand::ListConsumerGroupsOffsets(ListConsumerGroupsOffsetsCommand {
                client_id: action.client_id.clone(),
                operation_id: action.operation_id.clone(),
                groups: action
                    .groups
                    .iter()
                    .map(|group| ConsumerGroupOffsetsSelection {
                        group_id: group.group_id.clone(),
                        partitions: group
                            .partitions
                            .iter()
                            .map(|value| ConsumerGroupOffsetSelection {
                                topic: value.topic.clone(),
                                partition: value.partition,
                            })
                            .collect(),
                    })
                    .collect(),
                require_stable: action.require_stable,
                timeout_ms: action.timeout_ms,
            }),
            ExpectedEvent::ConsumerGroupsOffsetsListed {
                operation_id: action.operation_id.clone(),
            },
        ),
        ScenarioAction::AlterConsumerGroupOffsets(action) => (
            AdapterCommand::AlterConsumerGroupOffsets(
                testlab_schema::AlterConsumerGroupOffsetsCommand {
                    client_id: action.client_id.clone(),
                    operation_id: action.operation_id.clone(),
                    group_id: action.group_id.clone(),
                    offsets: action.offsets.clone(),
                    timeout_ms: action.timeout_ms,
                },
            ),
            ExpectedEvent::ConsumerGroupOffsetsAltered {
                operation_id: action.operation_id.clone(),
            },
        ),
        ScenarioAction::DeleteConsumerGroupOffsets(action) => (
            AdapterCommand::DeleteConsumerGroupOffsets(
                testlab_schema::DeleteConsumerGroupOffsetsCommand {
                    client_id: action.client_id.clone(),
                    operation_id: action.operation_id.clone(),
                    group_id: action.group_id.clone(),
                    partitions: action.partitions.clone(),
                    timeout_ms: action.timeout_ms,
                },
            ),
            ExpectedEvent::ConsumerGroupOffsetsDeleted {
                operation_id: action.operation_id.clone(),
            },
        ),
        ScenarioAction::DescribeClassicGroups(action) => (
            AdapterCommand::DescribeClassicGroups(DescribeClassicGroupsCommand {
                client_id: action.client_id.clone(),
                operation_id: action.operation_id.clone(),
                group_ids: action
                    .groups
                    .iter()
                    .map(|group| group.group_id.clone())
                    .collect(),
                timeout_ms: action.timeout_ms,
            }),
            ExpectedEvent::ClassicGroupsDescribed {
                operation_id: action.operation_id.clone(),
            },
        ),
        _ => return None,
    })
}
