//! Delegation-token observation targets retain only non-secret scenario selection.

use testlab_schema::{AdapterCommand, ScenarioAction};

use crate::observer_admin_target::{AdminTarget, TargetMatch};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct DelegationTokenTarget {
    pub(super) operation_id: testlab_schema::OperationId,
    pub(super) owner: testlab_schema::DelegationTokenPrincipalSpec,
}

pub(super) fn match_action(action: &ScenarioAction) -> Option<TargetMatch> {
    let ScenarioAction::ExerciseDelegationTokenLifecycle(action) = action else {
        return None;
    };
    Some((
        AdapterCommand::ExerciseDelegationTokenLifecycle(action.clone()),
        AdminTarget::DelegationTokens(DelegationTokenTarget {
            operation_id: action.operation_id.clone(),
            owner: action.owner.clone(),
        }),
    ))
}
