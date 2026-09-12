//! Group checkpoint selection defaults canonically and retains explicit compatibility use.

use crate::{GroupCheckpointMethod, Scenario, ScenarioAction};

#[test]
fn checked_in_group_scenarios_retain_both_full_checkpoint_paths() {
    let classic = scenario(include_str!(
        "../../../scenarios/kafka/classic-group-round-trip.toml"
    ));
    let modern = scenario(include_str!(
        "../../../scenarios/kafka/consumer-protocol-group-round-trip.toml"
    ));

    assert_eq!(method(&classic), GroupCheckpointMethod::IntoCheckpoint);
    assert_eq!(method(&modern), GroupCheckpointMethod::Checkpoint);
}

fn scenario(source: &str) -> Scenario {
    let scenario: Scenario = toml::from_str(source)
        .unwrap_or_else(|error| panic!("parse group checkpoint scenario: {error}"));
    scenario
        .validate()
        .unwrap_or_else(|error| panic!("validate group checkpoint scenario: {error}"));
    scenario
}

fn method(scenario: &Scenario) -> GroupCheckpointMethod {
    scenario
        .steps
        .iter()
        .find_map(|step| match &step.action {
            ScenarioAction::GroupReceive {
                checkpoint_method, ..
            } => Some(*checkpoint_method),
            _ => None,
        })
        .unwrap_or_else(|| panic!("group receive missing"))
}
