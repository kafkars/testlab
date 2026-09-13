//! Broker-unregistration translation keeps topology expectations scenario-local.

use testlab_schema::{AdapterCommand, ScenarioAction, UnregisterBrokerCommand};

use crate::runner_protocol::ExpectedEvent;

pub(crate) fn translate(action: &ScenarioAction) -> Option<(AdapterCommand, ExpectedEvent)> {
    let ScenarioAction::UnregisterBroker(action) = action else {
        return None;
    };
    Some((
        AdapterCommand::UnregisterBroker(UnregisterBrokerCommand {
            client_id: action.client_id.clone(),
            operation_id: action.operation_id.clone(),
            broker_id: action.broker_id,
            timeout_ms: action.timeout_ms,
        }),
        ExpectedEvent::BrokerUnregistered(action.operation_id.clone(), action.broker_id),
    ))
}
