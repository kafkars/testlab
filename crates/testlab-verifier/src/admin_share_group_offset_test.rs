//! Share-group offset verification joins one public result to immediate Kafka CLI state.

use std::collections::BTreeSet;

use testlab_schema::{
    AdapterCommand, AdapterEvent, AdminShareGroupOffsetListing, BrokerShareGroupOffset,
    BrokerStateObservation, Capability, ClientId, HistoryEntry, HistoryPayload,
    ListShareGroupOffsetsAction, ListShareGroupOffsetsCommand, OperationId,
    SCENARIO_SCHEMA_VERSION, Scenario, ScenarioAction, ScenarioId,
};

use crate::admin::verify_admin;
use crate::index::HistoryIndex;
use crate::verify_fixture::{command, event, step};

#[test]
fn exact_public_and_immediate_cli_offsets_pass() {
    assert!(violations(&history()).is_empty());
}

#[test]
fn unreported_cli_lag_still_corroborates_the_exact_start_offset() {
    let mut entries = history();
    let HistoryPayload::BrokerStateObservation { observation } = &mut entries[2].payload else {
        panic!("Share-group offset observation history kind");
    };
    let BrokerStateObservation::ShareGroupOffset(value) = observation else {
        panic!("Share-group offset observation kind");
    };
    value.lag = None;
    assert!(violations(&entries).is_empty());
}

#[test]
fn missing_public_topic_identity_fails_share_group_offset_contract() {
    let mut entries = history();
    let HistoryPayload::AdapterEvent { event } = &mut entries[1].payload else {
        panic!("Share-group offset event history kind");
    };
    let AdapterEvent::ShareGroupOffsetsListed(value) = &mut event.event else {
        panic!("Share-group offset event kind");
    };
    value.topic_id = [0; 16];
    assert_contract(&violations(&entries));
}

#[test]
fn mismatched_cli_lag_fails_share_group_offset_contract() {
    let mut entries = history();
    let HistoryPayload::BrokerStateObservation { observation } = &mut entries[2].payload else {
        panic!("Share-group offset observation history kind");
    };
    let BrokerStateObservation::ShareGroupOffset(value) = observation else {
        panic!("Share-group offset observation kind");
    };
    value.lag = Some(2);
    assert_contract(&violations(&entries));
}

fn history() -> Vec<HistoryEntry> {
    vec![
        command(1, AdapterCommand::ListShareGroupOffsets(command_value())),
        event(2, AdapterEvent::ShareGroupOffsetsListed(listing())),
        HistoryEntry {
            sequence: 3,
            observed_unix_ms: 3,
            payload: HistoryPayload::BrokerStateObservation {
                observation: BrokerStateObservation::ShareGroupOffset(BrokerShareGroupOffset {
                    observation: 0,
                    operation_id: operation(),
                    group_id: "share-group-1".to_owned(),
                    topic: "share-topic".to_owned(),
                    partition: 0,
                    start_offset: Some(1),
                    lag: Some(1),
                }),
            },
        },
    ]
}

fn listing() -> AdminShareGroupOffsetListing {
    AdminShareGroupOffsetListing {
        operation_id: operation(),
        group_id: "share-group-1".to_owned(),
        topic: "share-topic".to_owned(),
        partition: 0,
        topic_id: [1; 16],
        start_offset: Some(1),
        leader_epoch: Some(0),
        lag: Some(1),
        error_code: None,
    }
}

fn scenario() -> Scenario {
    Scenario {
        schema_version: SCENARIO_SCHEMA_VERSION,
        id: ScenarioId::new("kafka.admin-list-share-group-offsets")
            .unwrap_or_else(|error| panic!("scenario: {error}")),
        title: "Share-group offset listing".to_owned(),
        description: "public and independent Share-group offset state".to_owned(),
        timeout_ms: 10_000,
        requires: BTreeSet::from([Capability::Admin]),
        steps: vec![step(
            "list-share-group-offsets",
            ScenarioAction::ListShareGroupOffsets(action()),
        )],
        assertions: Vec::new(),
    }
}

fn action() -> ListShareGroupOffsetsAction {
    ListShareGroupOffsetsAction {
        client_id: client(),
        operation_id: operation(),
        group_id: "share-group-1".to_owned(),
        topic: "share-topic".to_owned(),
        partition: 0,
        expected_start_offset: 1,
        expected_lag: 1,
        timeout_ms: 1_000,
    }
}

fn command_value() -> ListShareGroupOffsetsCommand {
    ListShareGroupOffsetsCommand {
        client_id: client(),
        operation_id: operation(),
        group_id: "share-group-1".to_owned(),
        topic: "share-topic".to_owned(),
        partition: 0,
        timeout_ms: 1_000,
    }
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
            .any(|violation| violation.contract_id.as_str() == "ADMIN-038"),
        "{violations:?}"
    );
}

fn client() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client: {error}"))
}

fn operation() -> OperationId {
    OperationId::new("list-share-group-offsets")
        .unwrap_or_else(|error| panic!("operation: {error}"))
}
