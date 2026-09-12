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

    let error = scenario
        .validate()
        .expect_err("independent ownership without its capability must fail");

    assert!(error.problems.iter().any(|problem| {
        problem.contains("independent child handles require the independent_handles capability")
    }));
}
