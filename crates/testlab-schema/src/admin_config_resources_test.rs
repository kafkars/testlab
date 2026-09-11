//! Generic configuration-resource schemas retain exact filtered identities.

use std::collections::{BTreeMap, BTreeSet};

use crate::{
    AdapterCommand, AdapterEvent, AdminConfigResource, AdminConfigResourcesListing, ClientId,
    EVIDENCE_SCHEMA_VERSION, ListConfigResourcesAction, ListConfigResourcesCommand, OperationId,
    PROTOCOL_VERSION, SCENARIO_SCHEMA_VERSION, ScenarioAction,
};

#[test]
fn resource_listing_advances_all_versioned_boundaries() {
    assert_eq!(PROTOCOL_VERSION, 70);
    assert_eq!(SCENARIO_SCHEMA_VERSION, 73);
    assert_eq!(EVIDENCE_SCHEMA_VERSION, 59);
}

#[test]
fn action_command_and_listing_round_trip_without_expectation_leakage() {
    round_trip(&ScenarioAction::ListConfigResources(action()));
    round_trip(&AdapterCommand::ListConfigResources(command()));
    round_trip(&AdapterEvent::ConfigResourcesListed(listing()));
    let encoded = serde_json::to_string(&command())
        .unwrap_or_else(|error| panic!("encode resource listing: {error}"));
    assert!(!encoded.contains("required_topics"), "{encoded}");
}

#[test]
fn validation_rejects_duplicate_required_topics() {
    let mut action = action();
    action.required_topics[1] = action.required_topics[0].clone();
    let mut problems = Vec::new();
    crate::admin_config_action_validation::validate(
        &ScenarioAction::ListConfigResources(action),
        &BTreeMap::from([(client(), false)]),
        &mut BTreeSet::new(),
        &mut problems,
    );
    assert!(
        problems
            .iter()
            .any(|problem| problem.contains("unique valid names")),
        "{problems:?}"
    );
}

fn action() -> ListConfigResourcesAction {
    ListConfigResourcesAction {
        client_id: client(),
        operation_id: operation(),
        required_topics: vec!["topic-z".to_owned(), "topic-a".to_owned()],
        timeout_ms: 1_000,
    }
}

fn command() -> ListConfigResourcesCommand {
    ListConfigResourcesCommand {
        client_id: client(),
        operation_id: operation(),
        timeout_ms: 1_000,
    }
}

fn listing() -> AdminConfigResourcesListing {
    AdminConfigResourcesListing {
        operation_id: operation(),
        throttle_time_ms: 7,
        resources: vec![AdminConfigResource {
            resource_type: 2,
            name: "topic-a".to_owned(),
        }],
    }
}

fn client() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client: {error}"))
}

fn operation() -> OperationId {
    OperationId::new("list-config-resources").unwrap_or_else(|error| panic!("operation: {error}"))
}

fn round_trip<T>(value: &T)
where
    T: serde::Serialize + serde::de::DeserializeOwned + PartialEq + std::fmt::Debug,
{
    let json = serde_json::to_string(value)
        .unwrap_or_else(|error| panic!("serialize resource payload: {error}"));
    let decoded = serde_json::from_str(&json)
        .unwrap_or_else(|error| panic!("deserialize resource payload: {error}"));
    assert_eq!(value, &decoded);
}
