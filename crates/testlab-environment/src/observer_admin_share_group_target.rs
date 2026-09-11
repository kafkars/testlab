//! Share-group observation targets require an exact action and wire command pair.

use testlab_schema::{
    AdapterCommand, DescribeShareGroupCommand, ListShareGroupOffsetsCommand, ScenarioAction,
};

use crate::observer_admin_target::{
    AdminTarget, ShareGroupOffsetTarget, ShareGroupTarget, TargetMatch,
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
        _ => return Ok(None),
    }))
}
