//! Ownership tests preserve scenario compatibility and protocol explicitness.

use super::ChildHandleOwnership;
use crate::{AdapterCommand, Capability, SCENARIO_SCHEMA_VERSION, Scenario, ScenarioAction};

#[test]
fn scenario_actions_default_to_shared_ownership() {
    let action: ScenarioAction = serde_json::from_str(
        r#"{"kind":"create_producer","client_id":"client-1","producer_id":"producer-1"}"#,
    )
    .unwrap_or_else(|error| panic!("parse legacy scenario action: {error}"));

    assert!(matches!(
        action,
        ScenarioAction::CreateProducer {
            ownership: ChildHandleOwnership::Shared,
            delivery_timeout_ms: None,
            ..
        }
    ));
}

#[test]
fn protocol_commands_require_explicit_ownership() {
    let command = r#"{"kind":"create_producer","client_id":"client-1","producer_id":"producer-1"}"#;

    assert!(serde_json::from_str::<AdapterCommand>(command).is_err());
}

#[test]
fn independent_actions_require_the_matching_capability() {
    let mut scenario: Scenario = toml::from_str(include_str!(
        "../../../scenarios/kafka/producer-sibling-close-isolation.toml"
    ))
    .unwrap_or_else(|error| panic!("parse independent ownership scenario: {error}"));
    scenario.schema_version = SCENARIO_SCHEMA_VERSION;
    scenario.requires.remove(&Capability::IndependentHandles);

    let error = scenario.validation_error("independent ownership without its capability must fail");

    assert!(error.problems.iter().any(|problem| {
        problem.contains("independent child handles require the independent_handles capability")
    }));
}

#[test]
fn producer_handle_timeouts_are_bounded_and_capability_gated() {
    let mut scenario: Scenario = toml::from_str(include_str!(
        "../../../scenarios/kafka/producer-round-trip.toml"
    ))
    .unwrap_or_else(|error| panic!("parse producer timeout scenario: {error}"));
    scenario.schema_version = SCENARIO_SCHEMA_VERSION;
    assert!(scenario.validate().is_ok());

    scenario.requires.remove(&Capability::ProducerConfiguration);
    let error =
        scenario.validation_error("producer handle configuration must require its capability");
    assert!(error.problems.iter().any(|problem| {
        problem.contains("producer configuration requires the producer_configuration capability")
    }));

    scenario.requires.insert(Capability::ProducerConfiguration);
    for invalid in [99, 60_001] {
        let ScenarioAction::CreateProducer {
            delivery_timeout_ms,
            ..
        } = &mut scenario.steps[2].action
        else {
            panic!("producer creation fixture");
        };
        *delivery_timeout_ms = Some(invalid);
        let error = scenario.validation_error("invalid producer handle timeout must fail");
        assert!(error.problems.iter().any(|problem| {
            problem.contains("producer handle delivery_timeout_ms must be 100..=60000")
        }));
    }
}
