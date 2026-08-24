//! Consumer-group offset schema tests preserve intent, wire facts, and independent evidence.

use std::collections::{BTreeMap, BTreeSet};

use super::{
    AdapterCommand, AdapterEvent, BrokerStateObservation, Capability, ClientId,
    EVIDENCE_SCHEMA_VERSION, HistoryPayload, ListConsumerGroupOffsetsAction,
    ListConsumerGroupOffsetsCommand, OperationId, PROTOCOL_VERSION, SCENARIO_SCHEMA_VERSION,
    ScenarioAction,
};
use crate::admin_action_validation::validate;

#[test]
fn versions_advance_together_for_the_new_evidence_boundary() {
    assert_eq!(PROTOCOL_VERSION, 16);
    assert_eq!(SCENARIO_SCHEMA_VERSION, 14);
    assert_eq!(EVIDENCE_SCHEMA_VERSION, 6);
}

#[test]
fn scenario_expectation_stays_out_of_the_flat_wire_command() {
    let action = ScenarioAction::ListConsumerGroupOffsets(action(1));
    let command = AdapterCommand::ListConsumerGroupOffsets(command());

    let action_encoded = encode(&action);
    let command_encoded = encode(&command);

    assert!(action_encoded.contains("kind = \"list_consumer_group_offsets\""));
    assert!(action_encoded.contains("require_stable = true"));
    assert!(action_encoded.contains("expected_offset = 1"));
    assert_eq!(decode::<ScenarioAction>(&action_encoded), action);
    assert!(command_encoded.contains("kind = \"list_consumer_group_offsets\""));
    assert!(command_encoded.contains("require_stable = true"));
    assert!(!command_encoded.contains("expected_offset"));
    assert_eq!(decode::<AdapterCommand>(&command_encoded), command);
}

#[test]
fn command_and_action_reject_unknown_or_missing_fields() {
    let command_encoded = encode(&AdapterCommand::ListConsumerGroupOffsets(command()));
    let action_encoded = encode(&ScenarioAction::ListConsumerGroupOffsets(action(1)));

    assert!(
        toml::from_str::<AdapterCommand>(&format!("{command_encoded}unexpected = true\n")).is_err()
    );
    assert!(
        toml::from_str::<ScenarioAction>(&format!("{action_encoded}unexpected = true\n")).is_err()
    );
    assert!(
        toml::from_str::<AdapterCommand>(&command_encoded.replace("require_stable = true\n", ""))
            .is_err()
    );
}

#[test]
fn adapter_event_reports_only_the_public_group_offset_fact() {
    let event = AdapterEvent::ConsumerGroupOffsetListed {
        operation_id: operation(),
        group_id: "group-1".to_owned(),
        topic: "records".to_owned(),
        partition: 0,
        offset: Some(1),
    };

    let encoded = encode(&event);

    assert!(encoded.contains("kind = \"consumer_group_offset_listed\""));
    assert!(encoded.contains("group_id = \"group-1\""));
    assert!(encoded.contains("offset = 1"));
    assert!(!encoded.contains("expected_offset"));
    assert!(!encoded.contains("require_stable"));
    assert_eq!(decode::<AdapterEvent>(&encoded), event);
}

#[test]
fn broker_state_observation_is_typed_and_history_addressable() {
    let observation = BrokerStateObservation::ConsumerGroupOffset {
        observation: 7,
        operation_id: operation(),
        group_id: "group-1".to_owned(),
        topic: "records".to_owned(),
        partition: 0,
        offset: Some(1),
    };
    let history = HistoryPayload::BrokerStateObservation {
        observation: observation.clone(),
    };

    let observation_encoded = encode(&observation);
    let history_encoded = encode(&history);

    assert!(observation_encoded.contains("kind = \"consumer_group_offset\""));
    assert!(observation_encoded.contains("observation = 7"));
    assert_eq!(
        decode::<BrokerStateObservation>(&observation_encoded),
        observation
    );
    assert!(history_encoded.contains("source = \"broker_state_observation\""));
    assert!(history_encoded.contains("kind = \"consumer_group_offset\""));
    assert_eq!(decode::<HistoryPayload>(&history_encoded), history);
}

#[test]
fn validation_accepts_inclusive_bounds_and_records_admin_usage() {
    let client_id = client();
    let clients = BTreeMap::from([(client_id.clone(), false)]);
    let mut operation_ids = BTreeSet::new();
    let mut problems = Vec::new();
    let action = ScenarioAction::ListConsumerGroupOffsets(ListConsumerGroupOffsetsAction {
        client_id,
        operation_id: operation(),
        group_id: "g".repeat(255),
        topic: "t".repeat(249),
        partition: 0,
        require_stable: false,
        expected_offset: 0,
        timeout_ms: 100,
    });

    validate(&action, &clients, &mut operation_ids, &mut problems);
    let mut usage = BTreeSet::new();
    crate::scenario_capability_validation::record_usage(&action, &mut usage);

    assert!(problems.is_empty(), "{problems:?}");
    assert_eq!(operation_ids, BTreeSet::from([operation()]));
    assert_eq!(usage, BTreeSet::from([Capability::Admin]));
}

#[test]
fn validation_rejects_invalid_identity_and_bounded_fields() {
    let duplicate = operation();
    let clients = BTreeMap::from([(client(), true)]);
    let mut operation_ids = BTreeSet::from([duplicate]);
    let mut problems = Vec::new();
    let action = ScenarioAction::ListConsumerGroupOffsets(ListConsumerGroupOffsetsAction {
        client_id: client(),
        operation_id: operation(),
        group_id: String::new(),
        topic: "t".repeat(250),
        partition: -1,
        require_stable: true,
        expected_offset: -1,
        timeout_ms: 99,
    });

    validate(&action, &clients, &mut operation_ids, &mut problems);

    for expected in [
        "uses shut down client",
        "duplicate operation id",
        "has invalid group_id",
        "has invalid topic",
        "partition must be nonnegative",
        "expected_offset must be nonnegative",
        "timeout_ms must be between 100 and 60000",
    ] {
        assert!(
            problems.iter().any(|problem| problem.contains(expected)),
            "missing {expected:?} in {problems:?}"
        );
    }
}

fn action(expected_offset: i64) -> ListConsumerGroupOffsetsAction {
    ListConsumerGroupOffsetsAction {
        client_id: client(),
        operation_id: operation(),
        group_id: "group-1".to_owned(),
        topic: "records".to_owned(),
        partition: 0,
        require_stable: true,
        expected_offset,
        timeout_ms: 1_000,
    }
}

fn command() -> ListConsumerGroupOffsetsCommand {
    ListConsumerGroupOffsetsCommand {
        client_id: client(),
        operation_id: operation(),
        group_id: "group-1".to_owned(),
        topic: "records".to_owned(),
        partition: 0,
        require_stable: true,
        timeout_ms: 1_000,
    }
}

fn encode<T: serde::Serialize>(value: &T) -> String {
    toml::to_string(value).unwrap_or_else(|error| panic!("serialize schema value: {error}"))
}

fn decode<T: serde::de::DeserializeOwned>(value: &str) -> T {
    toml::from_str(value).unwrap_or_else(|error| panic!("deserialize schema value: {error}"))
}

fn client() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client id: {error}"))
}

fn operation() -> OperationId {
    OperationId::new("admin-group-offset-1").unwrap_or_else(|error| panic!("operation id: {error}"))
}
