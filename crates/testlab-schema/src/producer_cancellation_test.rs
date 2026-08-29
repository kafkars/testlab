//! Producer cancellation tests pin capability, timeout, and uncertainty validation.

use crate::{Capability, Scenario, ScenarioAction, TerminalStatus};

#[test]
fn cancellation_scenario_preserves_race_dependent_terminal_truth() {
    let scenario = scenario();
    scenario
        .validate()
        .unwrap_or_else(|error| panic!("validate producer cancellation: {error}"));

    let mut missing_capability = scenario.clone();
    missing_capability
        .requires
        .remove(&Capability::ProducerCancellation);
    assert_problem(&missing_capability, "producer_cancellation capability");

    let mut fixed_terminal = scenario.clone();
    fixed_terminal.assertions[0].terminal = Some(TerminalStatus::Acknowledged);
    assert_problem(&fixed_terminal, "must not predeclare");
}

#[test]
fn cancellation_timeout_is_bounded() {
    let mut scenario = scenario();
    let Some(ScenarioAction::CancelProducerSend(action)) =
        scenario.steps.get_mut(3).map(|step| &mut step.action)
    else {
        panic!("cancellation action missing");
    };
    action.timeout_ms = 99;
    assert_problem(&scenario, "timeout_ms must be between 100 and 60000");
}

fn scenario() -> Scenario {
    toml::from_str(include_str!(
        "../../../scenarios/kafka/producer-cancellation.toml"
    ))
    .unwrap_or_else(|error| panic!("parse producer cancellation: {error}"))
}

fn assert_problem(scenario: &Scenario, expected: &str) {
    let error = match scenario.validate() {
        Ok(()) => panic!("producer cancellation fixture must be invalid"),
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
