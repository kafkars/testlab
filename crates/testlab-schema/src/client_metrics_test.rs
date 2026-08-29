//! Client metrics tests enforce capability and stable operation ownership.

use crate::{Capability, Scenario, ScenarioAction};

#[test]
fn checked_in_metrics_scenario_is_valid_and_explicit() {
    let scenario = scenario();
    scenario
        .validate()
        .unwrap_or_else(|error| panic!("validate client metrics: {error}"));
    let ScenarioAction::ObserveClientMetrics(action) = &scenario.steps[5].action else {
        panic!("client metrics action missing");
    };
    assert_eq!(action.minimum_produce_records, 1);
    assert!(action.require_idle_producer);
    assert!(action.require_accepting);
    assert!(action.require_healthy);
}

#[test]
fn metrics_capability_and_operation_identity_are_required() {
    let mut scenario = scenario();
    scenario.requires.remove(&Capability::ClientMetrics);
    assert_problem(&scenario, "client_metrics capability");
    scenario.requires.insert(Capability::ClientMetrics);
    let ScenarioAction::ObserveClientMetrics(action) = &mut scenario.steps[5].action else {
        panic!("client metrics action missing");
    };
    action.operation_id = scenario.assertions[0].operation_id.clone();
    assert_problem(&scenario, "duplicate operation id");
}

fn scenario() -> Scenario {
    toml::from_str(include_str!(
        "../../../scenarios/kafka/client-metrics-producer.toml"
    ))
    .unwrap_or_else(|error| panic!("parse client metrics: {error}"))
}

fn assert_problem(scenario: &Scenario, expected: &str) {
    let error = match scenario.validate() {
        Ok(()) => panic!("client metrics fixture must be invalid"),
        Err(error) => error,
    };
    assert!(
        error
            .problems
            .iter()
            .any(|problem| problem.contains(expected)),
        "missing {expected:?} in {:?}",
        error.problems
    );
}
