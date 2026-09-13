//! Tests for the observer admin broker unregistration target contract.

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
    let AdapterCommand::UnregisterBroker(unregister) = &mut command else {
        panic!("unexpected wire command");
    };
    unregister.broker_id = 2;
    assert!(AdminTarget::from_exact(action, &command).is_err());
}
