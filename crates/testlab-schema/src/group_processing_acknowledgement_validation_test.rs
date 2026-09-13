//! Tests for the group processing acknowledgement validation contract.

use crate::{Capability, Scenario, ScenarioAction};

fn scenario() -> Scenario {
    toml::from_str(include_str!(
        "../../../scenarios/kafka/classic-group-processing-acknowledgement.toml"
    ))
    .unwrap_or_else(|error| panic!("parse acknowledgement scenario: {error}"))
}

#[test]
fn checked_in_acknowledgement_scenario_is_valid_and_requires_capability() {
    let mut scenario = scenario();
    scenario
        .validate()
        .unwrap_or_else(|error| panic!("validate acknowledgement scenario: {error}"));
    assert!(
        scenario
            .requires
            .remove(&Capability::GroupConsumerAcknowledge)
    );
    assert!(
        scenario
            .validation_error("acknowledgement capability must be explicit")
            .to_string()
            .contains("group_consumer_acknowledge")
    );
}

#[test]
fn acknowledgement_window_must_straddle_processing_timeout() {
    let mut scenario = scenario();
    let delay = scenario
        .steps
        .iter_mut()
        .find_map(|step| match &mut step.action {
            ScenarioAction::GroupReceive {
                processing_acknowledgement_delay_ms,
                ..
            } => Some(processing_acknowledgement_delay_ms),
            _ => None,
        })
        .unwrap_or_else(|| panic!("group receive missing"));
    *delay = 2_000;
    assert!(
        scenario
            .validation_error("short acknowledgement window must fail")
            .to_string()
            .contains("must exceed processing_timeout_ms")
    );
}
