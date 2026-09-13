//! Fenced-broker scenario tests pin the reversible paired observation window.

use super::*;

#[test]
fn checked_in_fenced_broker_scenario_is_reversible() {
    scenario()
        .validate()
        .unwrap_or_else(|error| panic!("validate fenced-broker scenario: {error}"));
}

#[test]
fn detached_fenced_broker_expectation_is_rejected() {
    let mut scenario = scenario();
    scenario.steps.remove(3);

    let error = scenario.validation_error("scenario without its broker stop must fail");
    assert!(
        error
            .problems
            .iter()
            .any(|problem| problem.contains("fenced-broker expectations")),
        "{:?}",
        error.problems
    );
}

#[test]
fn duplicate_fenced_broker_expectations_are_rejected() {
    let mut scenario = scenario();
    let action = scenario
        .steps
        .iter_mut()
        .find_map(|step| match &mut step.action {
            ScenarioAction::DescribeCluster(action) if action.include_fenced_brokers => {
                Some(action)
            }
            _ => None,
        })
        .unwrap_or_else(|| panic!("included fenced-broker description"));
    action.expected_fenced_broker_ids = vec![3, 3];

    let error = scenario.validation_error("duplicate fenced broker must fail");
    assert!(
        error
            .problems
            .iter()
            .any(|problem| problem.contains("strictly increasing")),
        "{:?}",
        error.problems
    );
}

fn scenario() -> Scenario {
    toml::from_str(include_str!(
        "../../../scenarios/kafka/admin-describe-fenced-brokers.toml"
    ))
    .unwrap_or_else(|error| panic!("parse fenced-broker scenario: {error}"))
}
