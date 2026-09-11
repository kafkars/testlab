//! Assigned-consumer configuration retains policy and capability ownership.

use crate::{AssignedConsumerReadIsolation, Capability, Scenario, ScenarioAction};

#[test]
fn checked_in_read_committed_policy_is_explicit_and_valid() {
    let scenario = scenario();
    scenario
        .validate()
        .unwrap_or_else(|error| panic!("validate assigned-consumer configuration: {error}"));
    let Some(ScenarioAction::CreateAssignedConsumerClient(action)) =
        scenario.steps.first().map(|step| &step.action)
    else {
        panic!("configured assigned-consumer client missing");
    };
    assert_eq!(
        action.configuration.read_isolation,
        AssignedConsumerReadIsolation::ReadCommitted
    );
}

#[test]
fn assigned_consumer_configuration_requires_its_capability() {
    let mut scenario = scenario();
    scenario
        .requires
        .remove(&Capability::AssignedConsumerConfiguration);
    let error = scenario
        .validate()
        .expect_err("assigned-consumer configuration capability must be required");
    assert!(
        error
            .problems
            .iter()
            .any(|problem| problem.contains("assigned_consumer_configuration")),
        "{:?}",
        error.problems
    );
}

fn scenario() -> Scenario {
    toml::from_str(include_str!(
        "../../../scenarios/kafka/assigned-consumer-read-committed.toml"
    ))
    .unwrap_or_else(|error| panic!("parse assigned-consumer configuration: {error}"))
}
