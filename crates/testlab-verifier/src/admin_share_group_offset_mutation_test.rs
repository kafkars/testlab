//! Share-group offset mutation verification requires close, baseline, and CLI post-state.

use std::collections::BTreeSet;

use testlab_schema::{
    AdapterCommand, AdapterEvent, AdminShareGroupOffsetAlteration, AdminShareGroupOffsetListing,
    AlterShareGroupOffsetsAction, AlterShareGroupOffsetsCommand, BrokerShareGroupOffset,
    BrokerStateObservation, Capability, ClientId, ConsumerId, HistoryEntry, HistoryPayload,
    ListShareGroupOffsetsAction, ListShareGroupOffsetsCommand, OperationId,
    SCENARIO_SCHEMA_VERSION, Scenario, ScenarioAction, ScenarioId,
};

use crate::admin::verify_admin;
use crate::index::HistoryIndex;
use crate::verify_fixture::{command, event, step};

#[test]
fn exact_closed_group_baseline_and_post_state_pass() {
    assert!(violations(&history()).is_empty());
}

#[test]
fn unreported_cli_lag_still_corroborates_the_exact_transition() {
    let mut entries = history();
    for entry in &mut entries {
        if let HistoryPayload::BrokerStateObservation {
            observation: BrokerStateObservation::ShareGroupOffset(value),
        } = &mut entry.payload
        {
            value.lag = None;
        }
    }
    assert!(violations(&entries).is_empty());
}

#[test]
fn missing_successful_member_close_fails_mutation_contract() {
    let entries = history()
        .into_iter()
        .filter(|entry| {
            !matches!(
                &entry.payload,
                HistoryPayload::AdapterEvent { event }
                    if matches!(&event.event, AdapterEvent::ShareConsumerClosed { .. })
            )
        })
        .collect::<Vec<_>>();
    assert_contract(&violations(&entries));
}

#[test]
fn zero_public_topic_identity_fails_mutation_contract() {
    let mut entries = history();
    let Some(value) = entries
        .iter_mut()
        .find_map(|entry| match &mut entry.payload {
            HistoryPayload::AdapterEvent { event } => match &mut event.event {
                AdapterEvent::ShareGroupOffsetsAltered(value) => Some(value),
                _ => None,
            },
            _ => None,
        })
    else {
        panic!("alteration event");
    };
    value.topic_id = [0; 16];
    assert_contract(&violations(&entries));
}

#[test]
fn mismatched_cli_post_state_fails_mutation_contract() {
    let mut entries = history();
    let Some(value) = entries
        .iter_mut()
        .find_map(|entry| match &mut entry.payload {
            HistoryPayload::BrokerStateObservation { observation } => match observation {
                BrokerStateObservation::ShareGroupOffset(value)
                    if value.operation_id == mutation_operation() =>
                {
                    Some(value)
                }
                _ => None,
            },
            _ => None,
        })
    else {
        panic!("mutation observation");
    };
    value.lag = Some(1);
    assert_contract(&violations(&entries));
}

fn history() -> Vec<HistoryEntry> {
    vec![
        command(1, AdapterCommand::ListShareGroupOffsets(baseline_command())),
        event(2, AdapterEvent::ShareGroupOffsetsListed(baseline_listing())),
        observation(3, baseline_operation(), 1, 1),
        command(
            4,
            AdapterCommand::CloseShareConsumer {
                consumer_id: consumer(),
            },
        ),
        event(
            5,
            AdapterEvent::ShareConsumerClosed {
                consumer_id: consumer(),
                success: true,
                delivery: None,
                code: None,
            },
        ),
        command(
            6,
            AdapterCommand::AlterShareGroupOffsets(mutation_command()),
        ),
        event(
            7,
            AdapterEvent::ShareGroupOffsetsAltered(AdminShareGroupOffsetAlteration {
                operation_id: mutation_operation(),
                group_id: "share-group-1".to_owned(),
                topic: "share-topic".to_owned(),
                partition: 0,
                topic_id: [2; 16],
                error_code: None,
            }),
        ),
        observation(8, mutation_operation(), 2, 0),
    ]
}

fn scenario() -> Scenario {
    Scenario {
        schema_version: SCENARIO_SCHEMA_VERSION,
        id: ScenarioId::new("kafka.admin-alter-share-group-offsets")
            .unwrap_or_else(|error| panic!("scenario: {error}")),
        title: "Share-group offset alteration".to_owned(),
        description: "closed group mutation from a corroborated baseline".to_owned(),
        timeout_ms: 10_000,
        requires: BTreeSet::from([Capability::Admin]),
        steps: vec![
            step(
                "create-share",
                ScenarioAction::CreateShareConsumer {
                    client_id: client(),
                    consumer_id: consumer(),
                    group_id: "share-group-1".to_owned(),
                    topics: vec!["share-topic".to_owned()],
                    rack: None,
                    membership_timeout_ms: 1_000,
                    close_timeout_ms: 1_000,
                    configuration: None,
                },
            ),
            step(
                "baseline",
                ScenarioAction::ListShareGroupOffsets(baseline_action()),
            ),
            step(
                "close-share",
                ScenarioAction::CloseShareConsumer {
                    consumer_id: consumer(),
                    expect_success: true,
                },
            ),
            step(
                "alter",
                ScenarioAction::AlterShareGroupOffsets(mutation_action()),
            ),
        ],
        assertions: Vec::new(),
    }
}

fn baseline_action() -> ListShareGroupOffsetsAction {
    ListShareGroupOffsetsAction {
        client_id: client(),
        operation_id: baseline_operation(),
        group_id: "share-group-1".to_owned(),
        topic: "share-topic".to_owned(),
        partition: 0,
        expected_start_offset: 1,
        expected_lag: 1,
        timeout_ms: 1_000,
    }
}

fn baseline_command() -> ListShareGroupOffsetsCommand {
    ListShareGroupOffsetsCommand {
        client_id: client(),
        operation_id: baseline_operation(),
        group_id: "share-group-1".to_owned(),
        topic: "share-topic".to_owned(),
        partition: 0,
        timeout_ms: 1_000,
    }
}

fn baseline_listing() -> AdminShareGroupOffsetListing {
    AdminShareGroupOffsetListing {
        operation_id: baseline_operation(),
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

fn mutation_action() -> AlterShareGroupOffsetsAction {
    AlterShareGroupOffsetsAction {
        client_id: client(),
        operation_id: mutation_operation(),
        group_id: "share-group-1".to_owned(),
        topic: "share-topic".to_owned(),
        partition: 0,
        start_offset: 2,
        expected_lag: 0,
        timeout_ms: 1_000,
    }
}

fn mutation_command() -> AlterShareGroupOffsetsCommand {
    AlterShareGroupOffsetsCommand {
        client_id: client(),
        operation_id: mutation_operation(),
        group_id: "share-group-1".to_owned(),
        topic: "share-topic".to_owned(),
        partition: 0,
        start_offset: 2,
        timeout_ms: 1_000,
    }
}

fn observation(sequence: u64, operation_id: OperationId, start: i64, lag: i64) -> HistoryEntry {
    HistoryEntry {
        sequence,
        observed_unix_ms: sequence,
        payload: HistoryPayload::BrokerStateObservation {
            observation: BrokerStateObservation::ShareGroupOffset(BrokerShareGroupOffset {
                observation: sequence,
                operation_id,
                group_id: "share-group-1".to_owned(),
                topic: "share-topic".to_owned(),
                partition: 0,
                start_offset: Some(start),
                lag: Some(lag),
            }),
        },
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
            .any(|violation| violation.contract_id.as_str() == "ADMIN-039"),
        "{violations:?}"
    );
}

fn client() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client: {error}"))
}

fn consumer() -> ConsumerId {
    ConsumerId::new("share-1").unwrap_or_else(|error| panic!("consumer: {error}"))
}

fn baseline_operation() -> OperationId {
    OperationId::new("list-share-group-offset-before-alter")
        .unwrap_or_else(|error| panic!("operation: {error}"))
}

fn mutation_operation() -> OperationId {
    OperationId::new("alter-share-group-offsets")
        .unwrap_or_else(|error| panic!("operation: {error}"))
}
