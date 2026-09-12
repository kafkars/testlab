//! Share-group offset deletion requires close, baseline, public success, and CLI absence.

use std::collections::BTreeSet;

use testlab_schema::{
    AdapterCommand, AdapterEvent, AdminShareGroupOffsetDeletion, AdminShareGroupOffsetListing,
    BrokerShareGroupOffset, BrokerStateObservation, Capability, ClientId, ConsumerId,
    DeleteShareGroupOffsetsAction, DeleteShareGroupOffsetsCommand, HistoryEntry, HistoryPayload,
    ListShareGroupOffsetsAction, ListShareGroupOffsetsCommand, OperationId,
    SCENARIO_SCHEMA_VERSION, Scenario, ScenarioAction, ScenarioId,
};

use crate::admin::verify_admin;
use crate::index::HistoryIndex;
use crate::verify_fixture::{command, event, step};

#[test]
fn exact_closed_group_baseline_and_absence_pass() {
    assert!(violations(&history()).is_empty());
}

#[test]
fn remaining_cli_offset_fails_deletion_contract() {
    let mut entries = history();
    observed(&mut entries).start_offset = Some(1);
    observed(&mut entries).lag = Some(1);
    assert_contract(&violations(&entries));
}

#[test]
fn zero_public_topic_identity_fails_deletion_contract() {
    let mut entries = history();
    deleted(&mut entries).topic_id = [0; 16];
    assert_contract(&violations(&entries));
}

#[test]
fn missing_successful_member_close_fails_deletion_contract() {
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

fn history() -> Vec<HistoryEntry> {
    vec![
        command(1, AdapterCommand::ListShareGroupOffsets(baseline_command())),
        event(2, AdapterEvent::ShareGroupOffsetsListed(baseline_listing())),
        observation(3, baseline_operation(), Some(1), Some(1)),
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
        command(6, AdapterCommand::DeleteShareGroupOffsets(delete_command())),
        event(
            7,
            AdapterEvent::ShareGroupOffsetsDeleted(AdminShareGroupOffsetDeletion {
                operation_id: delete_operation(),
                group_id: "share-group-1".to_owned(),
                topic: "share-topic".to_owned(),
                topic_id: [2; 16],
                error_code: None,
            }),
        ),
        observation(8, delete_operation(), None, None),
    ]
}

fn scenario() -> Scenario {
    Scenario {
        schema_version: SCENARIO_SCHEMA_VERSION,
        id: ScenarioId::new("kafka.admin-delete-share-group-offsets")
            .unwrap_or_else(|error| panic!("scenario: {error}")),
        title: "Share-group offset deletion".to_owned(),
        description: "closed group deletion from a corroborated baseline".to_owned(),
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
                "delete",
                ScenarioAction::DeleteShareGroupOffsets(delete_action()),
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

fn delete_action() -> DeleteShareGroupOffsetsAction {
    DeleteShareGroupOffsetsAction {
        client_id: client(),
        operation_id: delete_operation(),
        group_id: "share-group-1".to_owned(),
        topic: "share-topic".to_owned(),
        partition: 0,
        timeout_ms: 1_000,
    }
}

fn delete_command() -> DeleteShareGroupOffsetsCommand {
    DeleteShareGroupOffsetsCommand {
        client_id: client(),
        operation_id: delete_operation(),
        group_id: "share-group-1".to_owned(),
        topic: "share-topic".to_owned(),
        timeout_ms: 1_000,
    }
}

fn observation(
    sequence: u64,
    operation_id: OperationId,
    start_offset: Option<i64>,
    lag: Option<i64>,
) -> HistoryEntry {
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
                start_offset,
                lag,
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

fn observed(entries: &mut [HistoryEntry]) -> &mut BrokerShareGroupOffset {
    let HistoryPayload::BrokerStateObservation { observation } = &mut entries[7].payload else {
        panic!("deletion observation");
    };
    let BrokerStateObservation::ShareGroupOffset(value) = observation else {
        panic!("Share-group offset observation");
    };
    value
}

fn deleted(entries: &mut [HistoryEntry]) -> &mut AdminShareGroupOffsetDeletion {
    let HistoryPayload::AdapterEvent { event } = &mut entries[6].payload else {
        panic!("deletion event");
    };
    let AdapterEvent::ShareGroupOffsetsDeleted(value) = &mut event.event else {
        panic!("Share-group offset deletion");
    };
    value
}

fn assert_contract(violations: &[testlab_schema::Violation]) {
    assert!(
        violations
            .iter()
            .any(|violation| violation.contract_id.as_str() == "ADMIN-040"),
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
    OperationId::new("list-share-group-offset-before-delete")
        .unwrap_or_else(|error| panic!("operation: {error}"))
}

fn delete_operation() -> OperationId {
    OperationId::new("delete-share-group-offsets")
        .unwrap_or_else(|error| panic!("operation: {error}"))
}
