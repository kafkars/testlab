//! Cluster, consumer-group, and committed-offset actions produce exact observation targets.

use testlab_schema::{
    AdapterCommand, AlterConsumerGroupOffsetCommand, DeleteConsumerGroupCommand,
    DeleteConsumerGroupOffsetCommand, DescribeClusterCommand, DescribeConsumerGroupCommand,
    ListConsumerGroupOffsetsCommand, ListConsumerGroupsCommand, ScenarioAction,
};

use crate::observer_admin_target::{
    AdminTarget, GroupTarget, ListTarget, OffsetTarget, TargetMatch, unique,
};
use crate::observer_error::ObserverError;

#[allow(
    clippy::too_many_lines,
    reason = "the exhaustive group-admin matcher keeps exact action and command pairs adjacent"
)]
pub(super) fn match_action(action: &ScenarioAction) -> Result<Option<TargetMatch>, ObserverError> {
    let matched = match action {
        ScenarioAction::DescribeCluster(action) => (
            AdapterCommand::DescribeCluster(DescribeClusterCommand {
                client_id: action.client_id.clone(),
                operation_id: action.operation_id.clone(),
                timeout_ms: action.timeout_ms,
            }),
            AdminTarget::Cluster(action.operation_id.clone()),
        ),
        ScenarioAction::ListConsumerGroups(action) => {
            unique(
                &action.required_group_ids,
                &action.operation_id,
                "consumer groups",
            )?;
            (
                AdapterCommand::ListConsumerGroups(ListConsumerGroupsCommand {
                    client_id: action.client_id.clone(),
                    operation_id: action.operation_id.clone(),
                    timeout_ms: action.timeout_ms,
                }),
                AdminTarget::ConsumerGroups(ListTarget {
                    operation_id: action.operation_id.clone(),
                    names: action.required_group_ids.clone(),
                }),
            )
        }
        ScenarioAction::DescribeConsumerGroup(action) => (
            AdapterCommand::DescribeConsumerGroup(DescribeConsumerGroupCommand {
                client_id: action.client_id.clone(),
                operation_id: action.operation_id.clone(),
                group_id: action.group_id.clone(),
                timeout_ms: action.timeout_ms,
            }),
            AdminTarget::ConsumerGroup(GroupTarget {
                operation_id: action.operation_id.clone(),
                group_id: action.group_id.clone(),
                expected_member_count: Some(action.expected_member_count),
                expected_exists: true,
                poll_expected: false,
            }),
        ),
        ScenarioAction::ListConsumerGroupOffsets(action) => (
            AdapterCommand::ListConsumerGroupOffsets(ListConsumerGroupOffsetsCommand {
                client_id: action.client_id.clone(),
                operation_id: action.operation_id.clone(),
                group_id: action.group_id.clone(),
                topic: action.topic.clone(),
                partition: action.partition,
                require_stable: action.require_stable,
                timeout_ms: action.timeout_ms,
            }),
            AdminTarget::ConsumerGroupOffset(OffsetTarget {
                operation_id: action.operation_id.clone(),
                group_id: action.group_id.clone(),
                topic: action.topic.clone(),
                partition: action.partition,
                expected_offset: Some(action.expected_offset),
                poll_expected: false,
            }),
        ),
        ScenarioAction::AlterConsumerGroupOffset(action) => (
            AdapterCommand::AlterConsumerGroupOffset(AlterConsumerGroupOffsetCommand {
                client_id: action.client_id.clone(),
                operation_id: action.operation_id.clone(),
                group_id: action.group_id.clone(),
                topic: action.topic.clone(),
                partition: action.partition,
                offset: action.offset,
                timeout_ms: action.timeout_ms,
            }),
            AdminTarget::ConsumerGroupOffset(OffsetTarget {
                operation_id: action.operation_id.clone(),
                group_id: action.group_id.clone(),
                topic: action.topic.clone(),
                partition: action.partition,
                expected_offset: Some(action.offset),
                poll_expected: true,
            }),
        ),
        ScenarioAction::DeleteConsumerGroupOffset(action) => (
            AdapterCommand::DeleteConsumerGroupOffset(DeleteConsumerGroupOffsetCommand {
                client_id: action.client_id.clone(),
                operation_id: action.operation_id.clone(),
                group_id: action.group_id.clone(),
                topic: action.topic.clone(),
                partition: action.partition,
                timeout_ms: action.timeout_ms,
            }),
            AdminTarget::ConsumerGroupOffset(OffsetTarget {
                operation_id: action.operation_id.clone(),
                group_id: action.group_id.clone(),
                topic: action.topic.clone(),
                partition: action.partition,
                expected_offset: None,
                poll_expected: true,
            }),
        ),
        ScenarioAction::DeleteConsumerGroup(action) => (
            AdapterCommand::DeleteConsumerGroup(DeleteConsumerGroupCommand {
                client_id: action.client_id.clone(),
                operation_id: action.operation_id.clone(),
                group_id: action.group_id.clone(),
                timeout_ms: action.timeout_ms,
            }),
            AdminTarget::ConsumerGroup(GroupTarget {
                operation_id: action.operation_id.clone(),
                group_id: action.group_id.clone(),
                expected_member_count: None,
                expected_exists: false,
                poll_expected: true,
            }),
        ),
        _ => return Ok(None),
    };
    Ok(Some(matched))
}
