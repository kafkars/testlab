//! Share-group observation targets require an exact action and wire command pair.

use testlab_schema::{AdapterCommand, DescribeShareGroupCommand, ScenarioAction};

use crate::observer_admin_target::{AdminTarget, ShareGroupTarget, TargetMatch};
use crate::observer_error::ObserverError;

pub(super) fn match_action(action: &ScenarioAction) -> Result<Option<TargetMatch>, ObserverError> {
    let ScenarioAction::DescribeShareGroup(action) = action else {
        return Ok(None);
    };
    Ok(Some((
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
    )))
}
