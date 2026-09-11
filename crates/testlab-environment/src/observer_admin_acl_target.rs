//! ACL actions become exact caller-ordered independent CLI observation targets.

use testlab_schema::{AdapterCommand, ScenarioAction};

use crate::observer_admin_target::{AclsTarget, AdminTarget, TargetMatch, unique};
use crate::observer_error::ObserverError;

pub(super) fn match_action(action: &ScenarioAction) -> Result<Option<TargetMatch>, ObserverError> {
    let (command, operation_id, bindings) = match action {
        ScenarioAction::CreateAcls(value) => (
            AdapterCommand::CreateAcls(value.clone()),
            value.operation_id.clone(),
            value.bindings.clone(),
        ),
        ScenarioAction::DescribeAcls(value) => (
            AdapterCommand::DescribeAcls(value.clone()),
            value.operation_id.clone(),
            vec![value.binding.clone()],
        ),
        ScenarioAction::DeleteAcls(value) => (
            AdapterCommand::DeleteAcls(value.clone()),
            value.operation_id.clone(),
            value.bindings.clone(),
        ),
        _ => return Ok(None),
    };
    unique(&bindings, &operation_id, "ACL bindings")?;
    Ok(Some((
        command,
        AdminTarget::Acls(AclsTarget {
            operation_id,
            bindings,
        }),
    )))
}
