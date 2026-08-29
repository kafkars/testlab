//! Share-configuration schema tests pin capability gating and structural bounds.

use crate::{Capability, Scenario, ScenarioAction, ShareConsumerFetchConfiguration};

fn configured_scenario() -> Scenario {
    toml::from_str(include_str!(
        "../../../scenarios/kafka/share-group-fetch-max-records.toml"
    ))
    .unwrap_or_else(|error| panic!("parse configured Share scenario: {error}"))
}

#[test]
fn checked_in_share_fetch_configuration_is_valid() {
    configured_scenario()
        .validate()
        .unwrap_or_else(|error| panic!("validate configured Share scenario: {error}"));
}

#[test]
fn configured_share_consumer_requires_its_exact_capability() {
    let mut scenario = configured_scenario();
    scenario
        .requires
        .remove(&Capability::ShareConsumerConfiguration);

    let error = match scenario.validate() {
        Ok(()) => panic!("missing Share configuration capability must fail"),
        Err(error) => error,
    };
    assert!(error.problems.iter().any(|problem| {
        problem.contains("configured share consumers require the share_consumer_configuration")
    }));
}

#[test]
fn share_fetch_configuration_and_acquisition_expectation_are_bounded() {
    let mut scenario = configured_scenario();
    let Some(ScenarioAction::CreateShareConsumer { configuration, .. }) = scenario
        .steps
        .iter_mut()
        .find_map(|step| match &mut step.action {
            action @ ScenarioAction::CreateShareConsumer { .. } => Some(action),
            _ => None,
        })
    else {
        panic!("configured Share create missing");
    };
    *configuration = Some(ShareConsumerFetchConfiguration {
        max_records: 0,
        batch_size: 32,
    });

    let error = match scenario.validate() {
        Ok(()) => panic!("invalid Share configuration must fail"),
        Err(error) => error,
    };
    assert!(
        error
            .problems
            .iter()
            .any(|problem| problem.contains("max_records"))
    );
    assert!(
        error
            .problems
            .iter()
            .any(|problem| problem.contains("batch_size"))
    );
}
