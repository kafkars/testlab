//! Tests for the admin broker unregistration contract.

use super::*;
use crate::{AdapterCommand, Scenario};

#[test]
fn checked_in_scenario_is_reversible_and_valid() {
    scenario()
        .validate()
        .unwrap_or_else(|error| panic!("validate broker-unregistration scenario: {error}"));
}

#[test]
fn scenario_expectations_do_not_cross_the_wire() {
    let scenario = scenario();
    let action = scenario
        .steps
        .iter()
        .find_map(|step| match &step.action {
            ScenarioAction::UnregisterBroker(action) => Some(action),
            _ => None,
        })
        .unwrap_or_else(|| panic!("missing broker-unregistration action"));
    let command = AdapterCommand::UnregisterBroker(UnregisterBrokerCommand {
        client_id: action.client_id.clone(),
        operation_id: action.operation_id.clone(),
        broker_id: action.broker_id,
        timeout_ms: action.timeout_ms,
    });
    let encoded = serde_json::to_string(&command)
        .unwrap_or_else(|error| panic!("encode broker-unregistration command: {error}"));
    assert!(!encoded.contains("expected_remaining_broker_ids"));
    assert!(!encoded.contains("baseline_operation_id"));
    assert!(!encoded.contains("restored_operation_id"));
}

#[test]
fn missing_stop_breaks_the_reversible_window() {
    let mut scenario = scenario();
    scenario.steps.remove(3);
    let error = scenario.validation_error("scenario without the matching stop must fail");
    assert!(
        error
            .problems
            .iter()
            .any(|problem| problem.contains("immediately bracketed"))
    );
}

fn scenario() -> Scenario {
    toml::from_str(include_str!(
        "../../../scenarios/kafka/admin-unregister-broker.toml"
    ))
    .unwrap_or_else(|error| panic!("parse broker-unregistration scenario: {error}"))
}
