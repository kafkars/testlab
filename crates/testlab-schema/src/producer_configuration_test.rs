//! Producer configuration tests cover every codec and portable limit validation.

use std::collections::BTreeSet;

use crate::{Capability, ProducerSendMethod, Scenario, ScenarioAction};

#[test]
fn checked_in_scenarios_cover_every_public_compression() {
    let scenarios = [
        scenario(include_str!(
            "../../../scenarios/kafka/producer-configuration-none.toml"
        )),
        scenario(include_str!(
            "../../../scenarios/kafka/producer-configuration-gzip.toml"
        )),
        scenario(include_str!(
            "../../../scenarios/kafka/producer-configuration-snappy.toml"
        )),
        scenario(include_str!(
            "../../../scenarios/kafka/producer-configuration-lz4.toml"
        )),
        scenario(include_str!(
            "../../../scenarios/kafka/producer-configuration-zstd.toml"
        )),
    ];
    let mut compressions = BTreeSet::new();
    for scenario in scenarios {
        scenario
            .validate()
            .unwrap_or_else(|error| panic!("validate producer configuration: {error}"));
        let ScenarioAction::CreateConfiguredClient(action) = &scenario.steps[0].action else {
            panic!("configured client action missing");
        };
        compressions.insert(action.configuration.compression);
    }
    assert_eq!(compressions.len(), 5);
}

#[test]
fn configuration_capability_and_limit_relationships_are_required() {
    let mut scenario = scenario(include_str!(
        "../../../scenarios/kafka/producer-configuration-gzip.toml"
    ));
    scenario.requires.remove(&Capability::ProducerConfiguration);
    assert_problem(&scenario, "producer_configuration capability");
    scenario.requires.insert(Capability::ProducerConfiguration);
    let ScenarioAction::CreateConfiguredClient(action) = &mut scenario.steps[0].action else {
        panic!("configured client action missing");
    };
    action.configuration.limits.batch_bytes = 2_000_000;
    assert_problem(&scenario, "batch_bytes <= request_bytes");
}

#[test]
fn waiting_send_is_explicit_and_requires_its_capability() {
    let mut scenario = scenario(include_str!(
        "../../../scenarios/kafka/producer-waiting-send.toml"
    ));
    scenario
        .validate()
        .unwrap_or_else(|error| panic!("validate waiting send: {error}"));
    assert!(scenario.steps.iter().any(|step| matches!(
        &step.action,
        ScenarioAction::Send {
            method: ProducerSendMethod::Send,
            ..
        }
    )));

    scenario.requires.remove(&Capability::ProducerWaitingSend);
    assert_problem(&scenario, "producer_waiting_send capability");
}

#[test]
fn ordinary_send_defaults_to_try_send() {
    let scenario = scenario(include_str!(
        "../../../scenarios/kafka/producer-configuration-none.toml"
    ));
    assert!(scenario.steps.iter().any(|step| matches!(
        &step.action,
        ScenarioAction::Send {
            method: ProducerSendMethod::TrySend,
            ..
        }
    )));
}

fn scenario(source: &str) -> Scenario {
    toml::from_str(source).unwrap_or_else(|error| panic!("parse producer configuration: {error}"))
}

fn assert_problem(scenario: &Scenario, expected: &str) {
    let error = match scenario.validate() {
        Ok(()) => panic!("producer configuration fixture must be invalid"),
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
