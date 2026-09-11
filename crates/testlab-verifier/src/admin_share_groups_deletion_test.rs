//! Plural Share-group deletion requires order, success, modeled closure, and CLI absence.

use testlab_schema::{
    AdapterCommand, AdapterEvent, AdminShareGroupDeletionOutcome, AdminShareGroupsDeletion,
    BrokerShareGroupState, BrokerStateObservation, ClientId, ConsumerId, DeleteShareGroupsCommand,
    HistoryEntry, HistoryPayload, OperationId, Scenario,
};

use crate::admin::verify_admin;
use crate::index::HistoryIndex;
use crate::verify_fixture::{command, event};

#[test]
fn ordered_successful_deletion_after_both_closes_passes() {
    assert!(violations(&history()).is_empty());
}

#[test]
fn reordered_public_outcomes_fail_the_deletion_contract() {
    let mut entries = history();
    deleted(&mut entries).outcomes.swap(0, 1);
    assert_contract(&violations(&entries));
}

#[test]
fn per_group_public_failure_fails_the_deletion_contract() {
    let mut entries = history();
    deleted(&mut entries).outcomes[1].error_code = Some("group_not_empty".to_owned());
    assert_contract(&violations(&entries));
}

#[test]
fn listed_group_or_missing_close_fails_the_deletion_contract() {
    let mut listed = history();
    observed(&mut listed, 1).exists = true;
    assert_contract(&violations(&listed));

    let missing_close = history()
        .into_iter()
        .filter(|entry| {
            !matches!(
                &entry.payload,
                HistoryPayload::AdapterEvent { event }
                    if matches!(
                        &event.event,
                        AdapterEvent::ShareConsumerClosed { consumer_id, .. }
                            if consumer_id == &consumer("share-a")
                    )
            )
        })
        .collect::<Vec<_>>();
    assert_contract(&violations(&missing_close));
}

fn history() -> Vec<HistoryEntry> {
    vec![
        command(
            1,
            AdapterCommand::CloseShareConsumer {
                consumer_id: consumer("share-z"),
            },
        ),
        event(
            2,
            AdapterEvent::ShareConsumerClosed {
                consumer_id: consumer("share-z"),
                success: true,
                delivery: None,
                code: None,
            },
        ),
        command(
            3,
            AdapterCommand::CloseShareConsumer {
                consumer_id: consumer("share-a"),
            },
        ),
        event(
            4,
            AdapterEvent::ShareConsumerClosed {
                consumer_id: consumer("share-a"),
                success: true,
                delivery: None,
                code: None,
            },
        ),
        command(5, AdapterCommand::DeleteShareGroups(command_payload())),
        event(
            6,
            AdapterEvent::ShareGroupsDeleted(AdminShareGroupsDeletion {
                operation_id: operation(),
                outcomes: group_ids()
                    .into_iter()
                    .map(|group_id| AdminShareGroupDeletionOutcome {
                        group_id,
                        error_code: None,
                    })
                    .collect(),
            }),
        ),
        observation(7, 7, "testlab-admin-share-group-delete-z", false),
        observation(8, 8, "testlab-admin-share-group-delete-a", false),
    ]
}

fn scenario() -> Scenario {
    toml::from_str(include_str!(
        "../../../scenarios/kafka/admin-delete-share-groups.toml"
    ))
    .unwrap_or_else(|error| panic!("parse Share-group deletion scenario: {error}"))
}

fn command_payload() -> DeleteShareGroupsCommand {
    DeleteShareGroupsCommand {
        client_id: client(),
        operation_id: operation(),
        group_ids: group_ids(),
        timeout_ms: 20_000,
    }
}

fn observation(sequence: u64, ordinal: u64, group_id: &str, exists: bool) -> HistoryEntry {
    HistoryEntry {
        sequence,
        observed_unix_ms: sequence,
        payload: HistoryPayload::BrokerStateObservation {
            observation: BrokerStateObservation::ShareGroup(BrokerShareGroupState {
                observation: ordinal,
                operation_id: operation(),
                group_id: group_id.to_owned(),
                exists,
                state: None,
                member_count: None,
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

fn deleted(entries: &mut [HistoryEntry]) -> &mut AdminShareGroupsDeletion {
    let HistoryPayload::AdapterEvent { event } = &mut entries[5].payload else {
        panic!("Share-group deletion event");
    };
    let AdapterEvent::ShareGroupsDeleted(value) = &mut event.event else {
        panic!("Share-group deletion completion");
    };
    value
}

fn observed(entries: &mut [HistoryEntry], index: usize) -> &mut BrokerShareGroupState {
    let HistoryPayload::BrokerStateObservation { observation } = &mut entries[6 + index].payload
    else {
        panic!("Share-group observation");
    };
    let BrokerStateObservation::ShareGroup(value) = observation else {
        panic!("Share-group state");
    };
    value
}

fn assert_contract(violations: &[testlab_schema::Violation]) {
    assert!(
        violations
            .iter()
            .any(|violation| violation.contract_id.as_str() == "ADMIN-041"),
        "{violations:?}"
    );
}

fn group_ids() -> Vec<String> {
    vec![
        "testlab-admin-share-group-delete-z".to_owned(),
        "testlab-admin-share-group-delete-a".to_owned(),
    ]
}

fn client() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client: {error}"))
}

fn consumer(value: &str) -> ConsumerId {
    ConsumerId::new(value).unwrap_or_else(|error| panic!("consumer: {error}"))
}

fn operation() -> OperationId {
    OperationId::new("admin-delete-share-groups-1")
        .unwrap_or_else(|error| panic!("operation: {error}"))
}
