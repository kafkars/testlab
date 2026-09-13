//! Configuration-resource verdicts require canonical public and independent identities.

use testlab_schema::{
    AdapterCommand, AdapterEvent, AdminConfigResource, AdminConfigResourcesListing,
    BrokerConfigResourcesState, BrokerStateObservation, BrokerTopicState, ClientId,
    ConfigResourceListingApi, HistoryEntry, HistoryPayload, ListConfigResourcesCommand,
    OperationId, Scenario,
};

use crate::admin::verify_admin;
use crate::index::HistoryIndex;
use crate::verify_fixture::{command, event};

#[test]
fn canonical_public_resources_and_independent_topics_pass() {
    assert!(violations(&history()).is_empty());
}

#[test]
fn wrong_type_order_or_missing_independent_topic_fails() {
    let mut wrong_type = history();
    listing(&mut wrong_type).resources[0].resource_type = 4;
    assert_contract(&violations(&wrong_type));

    let mut reordered = history();
    listing(&mut reordered).resources.swap(0, 1);
    assert_contract(&violations(&reordered));

    let mut missing = history();
    let HistoryPayload::BrokerStateObservation { observation } = &mut missing[3].payload else {
        panic!("topic observation");
    };
    let BrokerStateObservation::Topic(value) = observation else {
        panic!("topic state");
    };
    value.exists = false;
    assert_contract(&violations(&missing));
}

#[test]
fn dedicated_client_metrics_listing_matches_one_cli_snapshot() {
    assert!(client_metrics_violations(&client_metrics_history()).is_empty());
}

#[test]
fn client_metrics_public_or_independent_set_mismatch_fails() {
    let mut public_mismatch = client_metrics_history();
    client_metrics_listing(&mut public_mismatch).resources.pop();
    assert_client_metrics_contract(&client_metrics_violations(&public_mismatch));

    let mut independent_mismatch = client_metrics_history();
    let HistoryPayload::BrokerStateObservation { observation } =
        &mut independent_mismatch[2].payload
    else {
        panic!("client-metrics observation");
    };
    let BrokerStateObservation::ConfigResources(value) = observation else {
        panic!("configuration-resource state");
    };
    value.resources[0].resource_type = 2;
    assert_client_metrics_contract(&client_metrics_violations(&independent_mismatch));
}

fn history() -> Vec<HistoryEntry> {
    vec![
        command(1, AdapterCommand::ListConfigResources(command_payload())),
        event(2, AdapterEvent::ConfigResourcesListed(listing_value())),
        topic_state(3, 40, alpha()),
        topic_state(4, 41, zulu()),
    ]
}

fn command_payload() -> ListConfigResourcesCommand {
    ListConfigResourcesCommand {
        client_id: client(),
        operation_id: operation(),
        api: ConfigResourceListingApi::Resource,
        timeout_ms: 20_000,
    }
}

fn client_metrics_history() -> Vec<HistoryEntry> {
    vec![
        command(
            1,
            AdapterCommand::ListConfigResources(ListConfigResourcesCommand {
                client_id: client(),
                operation_id: client_metrics_operation(),
                api: ConfigResourceListingApi::ClientMetrics,
                timeout_ms: 20_000,
            }),
        ),
        event(
            2,
            AdapterEvent::ConfigResourcesListed(AdminConfigResourcesListing {
                operation_id: client_metrics_operation(),
                throttle_time_ms: 5,
                resources: vec![
                    client_metrics_resource(alpha()),
                    client_metrics_resource(zulu()),
                ],
            }),
        ),
        HistoryEntry {
            sequence: 3,
            observed_unix_ms: 3,
            payload: HistoryPayload::BrokerStateObservation {
                observation: BrokerStateObservation::ConfigResources(BrokerConfigResourcesState {
                    observation: 40,
                    operation_id: client_metrics_operation(),
                    resources: vec![
                        client_metrics_resource(alpha()),
                        client_metrics_resource(zulu()),
                    ],
                }),
            },
        },
    ]
}

fn client_metrics_resource(name: &str) -> AdminConfigResource {
    AdminConfigResource {
        resource_type: 16,
        name: name.to_owned(),
    }
}

fn listing_value() -> AdminConfigResourcesListing {
    AdminConfigResourcesListing {
        operation_id: operation(),
        throttle_time_ms: 5,
        resources: vec![resource(alpha()), resource(zulu())],
    }
}

fn resource(name: &str) -> AdminConfigResource {
    AdminConfigResource {
        resource_type: 2,
        name: name.to_owned(),
    }
}

fn topic_state(sequence: u64, observation: u64, topic: &str) -> HistoryEntry {
    HistoryEntry {
        sequence,
        observed_unix_ms: sequence,
        payload: HistoryPayload::BrokerStateObservation {
            observation: BrokerStateObservation::Topic(BrokerTopicState {
                observation,
                operation_id: operation(),
                topic: topic.to_owned(),
                exists: true,
                partitions: vec![0],
            }),
        },
    }
}

fn listing(entries: &mut [HistoryEntry]) -> &mut AdminConfigResourcesListing {
    let HistoryPayload::AdapterEvent { event } = &mut entries[1].payload else {
        panic!("resource-listing event");
    };
    let AdapterEvent::ConfigResourcesListed(value) = &mut event.event else {
        panic!("resource-listing payload");
    };
    value
}

fn violations(entries: &[HistoryEntry]) -> Vec<testlab_schema::Violation> {
    let index = HistoryIndex::build(entries);
    let mut violations = Vec::new();
    verify_admin(&scenario(), &index, &[], &mut violations);
    violations
}

fn client_metrics_violations(entries: &[HistoryEntry]) -> Vec<testlab_schema::Violation> {
    let index = HistoryIndex::build(entries);
    let mut violations = Vec::new();
    verify_admin(&client_metrics_scenario(), &index, &[], &mut violations);
    violations
}

fn assert_contract(violations: &[testlab_schema::Violation]) {
    assert!(
        violations
            .iter()
            .any(|violation| violation.contract_id.as_str() == "ADMIN-063"),
        "{violations:?}"
    );
}

fn assert_client_metrics_contract(violations: &[testlab_schema::Violation]) {
    assert!(
        violations
            .iter()
            .any(|violation| violation.contract_id.as_str() == "ADMIN-071"),
        "{violations:?}"
    );
}

fn scenario() -> Scenario {
    toml::from_str(
        r#"
schema_version = 144
id = "config-resources-fixture"
title = "configuration resources fixture"
description = "isolated verifier fixture"
timeout_ms = 10000
requires = ["admin"]
assertions = []

[[steps]]
id = "list"
kind = "list_config_resources"
client_id = "client-1"
operation_id = "admin-list-config-resources"
api = "resource"
required_resources = [
  "testlab-kafkars-config-resource-zulu",
  "testlab-kafkars-config-resource-alpha",
]
timeout_ms = 20000
"#,
    )
    .unwrap_or_else(|error| panic!("parse configuration-resource fixture: {error}"))
}

fn client_metrics_scenario() -> Scenario {
    toml::from_str(
        r#"
schema_version = 144
id = "client-metrics-resources-fixture"
title = "client metrics resources fixture"
description = "isolated verifier fixture"
timeout_ms = 10000
requires = ["admin"]
assertions = []

[[steps]]
id = "list"
kind = "list_config_resources"
client_id = "client-1"
operation_id = "admin-list-client-metrics-resources"
api = "client_metrics"
required_resources = ["testlab-kafkars-config-resource-zulu", "testlab-kafkars-config-resource-alpha"]
timeout_ms = 20000
"#,
    )
    .unwrap_or_else(|error| panic!("parse client-metrics resource fixture: {error}"))
}

fn client_metrics_listing(entries: &mut [HistoryEntry]) -> &mut AdminConfigResourcesListing {
    let HistoryPayload::AdapterEvent { event } = &mut entries[1].payload else {
        panic!("client-metrics listing event");
    };
    let AdapterEvent::ConfigResourcesListed(value) = &mut event.event else {
        panic!("client-metrics listing payload");
    };
    value
}

fn client() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client: {error}"))
}

fn operation() -> OperationId {
    OperationId::new("admin-list-config-resources")
        .unwrap_or_else(|error| panic!("operation: {error}"))
}

fn client_metrics_operation() -> OperationId {
    OperationId::new("admin-list-client-metrics-resources")
        .unwrap_or_else(|error| panic!("operation: {error}"))
}

fn alpha() -> &'static str {
    "testlab-kafkars-config-resource-alpha"
}

fn zulu() -> &'static str {
    "testlab-kafkars-config-resource-zulu"
}
