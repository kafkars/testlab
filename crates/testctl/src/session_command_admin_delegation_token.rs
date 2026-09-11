//! Delegation-token scenario intent maps to one exact secret-free command.

use testlab_schema::{AdapterCommand, ExerciseDelegationTokenLifecycleCommand, ScenarioAction};

use crate::runner_protocol::ExpectedEvent;

pub(crate) fn translate(action: &ScenarioAction) -> Option<(AdapterCommand, ExpectedEvent)> {
    let ScenarioAction::ExerciseDelegationTokenLifecycle(action) = action else {
        return None;
    };
    Some((
        AdapterCommand::ExerciseDelegationTokenLifecycle(ExerciseDelegationTokenLifecycleCommand {
            client_id: action.client_id.clone(),
            operation_id: action.operation_id.clone(),
            owner: action.owner.clone(),
            renewers: action.renewers.clone(),
            max_lifetime_ms: action.max_lifetime_ms,
            renew_period_ms: action.renew_period_ms,
            timeout_ms: action.timeout_ms,
        }),
        ExpectedEvent::DelegationTokenLifecycleExercised(action.operation_id.clone()),
    ))
}
