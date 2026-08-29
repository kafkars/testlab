//! Group-consumer shutdown tests pin capability, bounds, closure, and wire shape.

use crate::{Capability, Scenario, ScenarioAction};

#[test]
fn hosted_group_shutdown_round_trips_and_validates() {
    let scenario = shutdown_scenario();
    scenario
        .validate()
        .unwrap_or_else(|error| panic!("validate group shutdown: {error}"));
    let encoded = toml::to_string(&scenario)
        .unwrap_or_else(|error| panic!("serialize group shutdown: {error}"));
    let decoded: Scenario = toml::from_str(&encoded)
        .unwrap_or_else(|error| panic!("deserialize group shutdown: {error}"));
    assert_eq!(decoded, scenario);
}

#[test]
fn hosted_group_shutdown_requires_capability_and_bounded_requests() {
    let mut scenario = shutdown_scenario();
    scenario.requires.remove(&Capability::GroupConsumerShutdown);
    assert_problem(&scenario, "group_consumer_shutdown capability");
    scenario.requires.insert(Capability::GroupConsumerShutdown);
    let ScenarioAction::ShutdownGroupConsumer(action) = &mut scenario.steps[8].action else {
        panic!("group shutdown missing");
    };
    action.request_count = 0;
    action.timeout_ms = 99;
    assert_problem(&scenario, "request_count must be between 1 and 8");
    assert_problem(&scenario, "timeout_ms must be between 100 and 60000");
}

fn shutdown_scenario() -> Scenario {
    toml::from_str(include_str!(
        "../../../scenarios/kafka/classic-group-shutdown.toml"
    ))
    .unwrap_or_else(|error| panic!("parse group shutdown scenario: {error}"))
}

fn assert_problem(scenario: &Scenario, expected: &str) {
    let error = match scenario.validate() {
        Ok(()) => panic!("group shutdown fixture must be invalid"),
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
