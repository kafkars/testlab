//! Consumer-group batch deletion maps exact wire names to ordered absence targets.

use testlab_schema::{AdapterCommand, DeleteConsumerGroupsCommand, ScenarioAction};

use crate::observer_admin_target::{AdminTarget, ListTarget, TargetMatch, unique};
use crate::observer_error::ObserverError;

pub(super) fn match_action(action: &ScenarioAction) -> Result<Option<TargetMatch>, ObserverError> {
    let ScenarioAction::DeleteConsumerGroups(action) = action else {
        return Ok(None);
    };
    unique(&action.group_ids, &action.operation_id, "consumer groups")?;
    Ok(Some((
        AdapterCommand::DeleteConsumerGroups(DeleteConsumerGroupsCommand {
            client_id: action.client_id.clone(),
            operation_id: action.operation_id.clone(),
            group_ids: action.group_ids.clone(),
            timeout_ms: action.timeout_ms,
        }),
        AdminTarget::ConsumerGroupDeletions(ListTarget {
            operation_id: action.operation_id.clone(),
            names: action.group_ids.clone(),
        }),
    )))
}
