//! Broker-unregistration targets retain only independent postcondition expectations.

use testlab_schema::{AdapterCommand, ScenarioAction, UnregisterBrokerCommand};

use crate::observer_admin_target::{AdminTarget, TargetMatch};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct BrokerUnregistrationTarget {
    pub(super) operation_id: testlab_schema::OperationId,
    pub(super) expected_remaining_broker_ids: Vec<i32>,
}

pub(super) fn match_action(action: &ScenarioAction) -> Option<TargetMatch> {
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
        AdminTarget::BrokerUnregistration(BrokerUnregistrationTarget {
            operation_id: action.operation_id.clone(),
            expected_remaining_broker_ids: action.expected_remaining_broker_ids.clone(),
        }),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::observer_admin_target::AdminTarget;
    use testlab_schema::Scenario;

    #[test]
    fn exact_wire_command_retains_the_independent_target() {
        let scenario: Scenario = toml::from_str(include_str!(
            "../../../scenarios/kafka/admin-unregister-broker.toml"
        ))
        .unwrap_or_else(|error| panic!("parse broker-unregistration scenario: {error}"));
        let action = &scenario.steps[4].action;
        let (command, expected) =
            match_action(action).unwrap_or_else(|| panic!("match broker-unregistration action"));
        let target = AdminTarget::from_exact(action, &command)
            .unwrap_or_else(|error| panic!("match exact command: {error}"));
        assert_eq!(target, Some(expected));
    }

    #[test]
    fn changed_broker_id_rejects_observation() {
        let scenario: Scenario = toml::from_str(include_str!(
            "../../../scenarios/kafka/admin-unregister-broker.toml"
        ))
        .unwrap_or_else(|error| panic!("parse broker-unregistration scenario: {error}"));
        let action = &scenario.steps[4].action;
        let (mut command, _) =
            match_action(action).unwrap_or_else(|| panic!("match broker-unregistration action"));
        let AdapterCommand::UnregisterBroker(command) = &mut command else {
            panic!("unexpected wire command");
        };
        command.broker_id = 2;
        assert!(AdminTarget::from_exact(action, &command).is_err());
    }
}
