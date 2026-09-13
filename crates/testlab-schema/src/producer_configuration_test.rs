//! Producer configuration tests cover every codec and portable limit validation.

use std::collections::BTreeSet;

use crate::{
    AdapterEvent, Capability, ClientConfigurationObservation, ClientId,
    ProducerHandleConfigurationObservation, ProducerId, ProducerSendMethod, Scenario,
    ScenarioAction,
};

#[test]
fn configured_client_event_nests_selected_public_producer_policy() {
    let scenario = scenario(include_str!(
        "../../../scenarios/kafka/producer-configuration-gzip.toml"
    ));
    let ScenarioAction::CreateConfiguredClient(action) = &scenario.steps[0].action else {
        panic!("configured client action missing");
    };
    let event = AdapterEvent::ClientCreated(ClientConfigurationObservation {
        client_id: ClientId::new("client-gzip")
            .unwrap_or_else(|error| panic!("client ID: {error}")),
        observed_client_id: Some("client-gzip".to_owned()),
        observed_bootstrap_servers: vec!["127.0.0.1:9092".to_owned()],
        observed_expected_cluster_id: None,
        selected_producer_configuration: Some(action.configuration),
    });
    let value = serde_json::to_value(&event)
        .unwrap_or_else(|error| panic!("serialize client creation: {error}"));
    assert_eq!(
        value,
        serde_json::json!({
            "kind": "client_created",
            "client_id": "client-gzip",
            "observed_client_id": "client-gzip",
            "observed_bootstrap_servers": ["127.0.0.1:9092"],
            "observed_expected_cluster_id": null,
            "selected_producer_configuration": {
                "delivery_timeout_ms": 20000,
                "compression": "gzip",
                "max_retries": 3,
                "retry_backoff_ms": 10,
                "limits": {
                    "retained_bytes": 8_388_608,
                    "in_flight_records": 256,
                    "waiting_records": 128,
                    "waiting_bytes": 4_194_304,
                    "batch_records": 32,
                    "batch_bytes": 524_288,
                    "request_bytes": 1_048_576,
                    "max_in_flight_requests_per_broker": 4,
                    "linger_ms": 3,
                },
            },
        })
    );
    let decoded: AdapterEvent = serde_json::from_value(value)
        .unwrap_or_else(|error| panic!("deserialize client creation: {error}"));
    assert_eq!(decoded, event);
}

#[test]
fn producer_creation_event_flattens_selected_builder_policy() {
    let event = AdapterEvent::ProducerCreated(ProducerHandleConfigurationObservation {
        producer_id: ProducerId::new("producer-1")
            .unwrap_or_else(|error| panic!("producer ID: {error}")),
        selected_delivery_timeout_ms: 15_000,
    });
    let value = serde_json::to_value(&event)
        .unwrap_or_else(|error| panic!("serialize producer creation: {error}"));
    assert_eq!(
        value,
        serde_json::json!({
            "kind": "producer_created",
            "producer_id": "producer-1",
            "selected_delivery_timeout_ms": 15_000,
        })
    );
    let decoded: AdapterEvent = serde_json::from_value(value)
        .unwrap_or_else(|error| panic!("deserialize producer creation: {error}"));
    assert_eq!(decoded, event);
}

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
