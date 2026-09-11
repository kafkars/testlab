//! Configuration-resource verdicts require canonical public and independent topic identities.

use testlab_schema::{
    AdapterCommand, AdapterEvent, AdminConfigResource, AdminConfigResourcesListing,
    BrokerStateObservation, BrokerTopicState, ClientId, HistoryEntry, HistoryPayload,
    ListConfigResourcesCommand, OperationId, Scenario,
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
        timeout_ms: 20_000,
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

fn assert_contract(violations: &[testlab_schema::Violation]) {
    assert!(
        violations
            .iter()
            .any(|violation| violation.contract_id.as_str() == "ADMIN-063"),
        "{violations:?}"
    );
}

fn scenario() -> Scenario {
    toml::from_str(
        r#"
schema_version = 68
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
required_topics = [
  "testlab-kafkars-config-resource-zulu",
  "testlab-kafkars-config-resource-alpha",
]
timeout_ms = 20000
"#,
    )
    .unwrap_or_else(|error| panic!("parse configuration-resource fixture: {error}"))
}

fn client() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client: {error}"))
}

fn operation() -> OperationId {
    OperationId::new("admin-list-config-resources")
        .unwrap_or_else(|error| panic!("operation: {error}"))
}

fn alpha() -> &'static str {
    "testlab-kafkars-config-resource-alpha"
}

fn zulu() -> &'static str {
    "testlab-kafkars-config-resource-zulu"
}
