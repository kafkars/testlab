//! Broker-unregistration translation tests keep expectations scenario-local.

use testlab_schema::{AdapterCommand, Scenario};

use crate::runner_protocol::ExpectedEvent;
use crate::session_command_admin_broker_unregistration::translate;

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
