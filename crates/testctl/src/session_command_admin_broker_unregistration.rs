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

#[cfg(test)]
mod tests {
    use super::*;
    use testlab_schema::Scenario;

    #[test]
    fn translation_excludes_scenario_only_cluster_expectations() {
        let scenario: Scenario = toml::from_str(include_str!(
            "../../../scenarios/kafka/admin-unregister-broker.toml"
        ))
        .unwrap_or_else(|error| panic!("parse broker-unregistration scenario: {error}"));
        let action = &scenario.steps[4].action;
        let (command, expected) =
            translate(action).unwrap_or_else(|| panic!("translate broker-unregistration action"));
        let AdapterCommand::UnregisterBroker(command) = command else {
            panic!("unexpected wire command");
        };
        assert_eq!(command.broker_id, 3);
        assert_eq!(command.timeout_ms, 30_000);
        assert!(matches!(expected, ExpectedEvent::BrokerUnregistered(_, 3)));
    }
}
