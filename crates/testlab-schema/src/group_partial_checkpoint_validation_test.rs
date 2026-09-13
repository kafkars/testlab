//! Tests for the group partial checkpoint validation contract.

use crate::{Capability, GroupCheckpointMethod, Scenario, ScenarioAction};

fn scenario() -> Scenario {
    toml::from_str(include_str!(
        "../../../scenarios/kafka/classic-group-partial-checkpoint.toml"
    ))
    .unwrap_or_else(|error| panic!("parse partial checkpoint scenario: {error}"))
}

#[test]
fn checked_in_partial_checkpoint_is_valid_and_capability_gated() {
    let mut scenario = scenario();
    scenario
        .validate()
        .unwrap_or_else(|error| panic!("validate partial checkpoint scenario: {error}"));
    assert!(
        scenario
            .requires
            .remove(&Capability::GroupConsumerPartialCheckpoint)
    );
    assert!(
        scenario
            .validation_error("partial checkpoint capability must be explicit")
            .to_string()
            .contains("group_consumer_partial_checkpoint")
    );
}

#[test]
fn processed_count_must_leave_an_uncommitted_suffix() {
    let mut scenario = scenario();
    let count = scenario
        .steps
        .iter_mut()
        .find_map(|step| match &mut step.action {
            ScenarioAction::GroupReceive {
                processed_record_count,
                ..
            } => processed_record_count.as_mut(),
            _ => None,
        })
        .unwrap_or_else(|| panic!("partial group receive missing"));
    *count = 2;
    assert!(
        scenario
            .validation_error("full processed count must fail")
            .to_string()
            .contains("nonempty proper prefix")
    );
}

#[test]
fn partial_checkpoint_rejects_full_batch_conversion() {
    let mut scenario = scenario();
    let method = scenario
        .steps
        .iter_mut()
        .find_map(|step| match &mut step.action {
            ScenarioAction::GroupReceive {
                checkpoint_method,
                processed_record_count: Some(_),
                ..
            } => Some(checkpoint_method),
            _ => None,
        })
        .unwrap_or_else(|| panic!("partial group receive missing"));
    *method = GroupCheckpointMethod::IntoCheckpoint;
    assert!(
        scenario
            .validation_error("full conversion must fail for a partial checkpoint")
            .to_string()
            .contains("requires checkpoint_builder")
    );
}
