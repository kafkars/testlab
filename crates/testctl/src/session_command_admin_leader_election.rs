//! Leader-election actions translate without leaking transition expectations.

use testlab_schema::{AdapterCommand, ElectLeadersCommand, ScenarioAction};

use crate::runner_protocol::ExpectedEvent;

pub(crate) fn translate(action: &ScenarioAction) -> Option<(AdapterCommand, ExpectedEvent)> {
    let ScenarioAction::ElectLeaders(action) = action else {
        return None;
    };
    Some((
        AdapterCommand::ElectLeaders(ElectLeadersCommand {
            client_id: action.client_id.clone(),
            operation_id: action.operation_id.clone(),
            election_type: action.election_type,
            targets: action.targets.clone(),
            timeout_ms: action.timeout_ms,
        }),
        ExpectedEvent::LeadersElected(action.operation_id.clone()),
    ))
}
