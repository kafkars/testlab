//! Plural Share offset tests preserve nested public and independent order.

use testlab_schema::{
    AdapterCommand, AdapterEvent, AdminShareGroupOffsetOutcome, AdminShareGroupOffsetsOutcome,
    AdminShareGroupsOffsetsListing, BrokerShareGroupOffset, BrokerStateObservation, ClientId,
    HistoryEntry, HistoryPayload, ListShareGroupsOffsetsAction, ListShareGroupsOffsetsCommand,
    OperationId, ScenarioAction, ShareGroupOffsetExpectation, ShareGroupOffsetSelection,
    ShareGroupOffsetsExpectation, ShareGroupOffsetsSelection, TerminalStatus,
    VisibilityExpectation,
};

use crate::admin::verify_admin;
use crate::index::HistoryIndex;
use crate::verify_fixture::{command, event, scenario, step};

#[test]
fn exact_nested_plural_share_offset_listing_passes() {
    let action = list_action();
    assert!(violations(action.clone(), &list_history(&action)).is_empty());
}

#[test]
fn public_or_independent_reordering_fails_the_contract() {
    let action = list_action();
    let mut public = list_history(&action);
    listed(&mut public).groups.swap(0, 1);
    assert_contract(&violations(action.clone(), &public));

    let mut independent = list_history(&action);
    independent.swap(2, 3);
    assert_contract(&violations(action, &independent));
}

#[test]
fn group_or_partition_error_fails_the_contract() {
    let action = list_action();
    let mut group_error = list_history(&action);
    listed(&mut group_error).groups[0].error_code = Some("group_authorization_failed".to_owned());
    assert_contract(&violations(action.clone(), &group_error));

    let mut partition_error = list_history(&action);
    listed(&mut partition_error).groups[1].offsets[0].error_code =
        Some("unknown_topic_or_partition".to_owned());
    assert_contract(&violations(action, &partition_error));
}

#[test]
fn lossy_public_partition_state_fails_the_contract() {
    let action = list_action();
    let mut zero_topic = list_history(&action);
    listed(&mut zero_topic).groups[0].offsets[0].topic_id = [0; 16];
    assert_contract(&violations(action.clone(), &zero_topic));

    let mut missing_epoch = list_history(&action);
    listed(&mut missing_epoch).groups[1].offsets[0].leader_epoch = None;
    assert_contract(&violations(action, &missing_epoch));
}

#[test]
fn wrong_or_noncontiguous_independent_state_fails_the_contract() {
    let action = list_action();
    let mut wrong = list_history(&action);
    offset_observation(&mut wrong[2]).lag = Some(99);
    assert_contract(&violations(action.clone(), &wrong));

    let mut noncontiguous = list_history(&action);
    offset_observation(&mut noncontiguous[3]).observation = 99;
    assert_contract(&violations(action, &noncontiguous));
}

#[test]
fn altered_nested_wire_selection_fails_the_contract() {
    let action = list_action();
    let mut history = list_history(&action);
    let HistoryPayload::HarnessCommand { command } = &mut history[0].payload else {
        panic!("plural Share offset command");
    };
    let AdapterCommand::ListShareGroupsOffsets(command) = &mut command.command else {
        panic!("plural Share offset payload");
    };
    command.groups[1].partitions[0].partition = 9;
    assert_contract(&violations(action, &history));
}

fn list_action() -> ScenarioAction {
    ScenarioAction::ListShareGroupsOffsets(ListShareGroupsOffsetsAction {
        client_id: client(),
        operation_id: operation(),
        groups: vec![
            group("share-z", [("topic-z", 1, 4, 6)].as_slice()),
            group(
                "share-a",
                [("topic-a", 0, 2, 7), ("topic-c", 2, 9, 1)].as_slice(),
            ),
        ],
        timeout_ms: 2_000,
    })
}

fn list_history(action: &ScenarioAction) -> Vec<HistoryEntry> {
    let ScenarioAction::ListShareGroupsOffsets(listing) = action else {
        panic!("plural Share offset action fixture");
    };
    let groups = listing
        .groups
        .iter()
        .map(|group| ShareGroupOffsetsSelection {
            group_id: group.group_id.clone(),
            partitions: group
                .partitions
                .iter()
                .map(|value| ShareGroupOffsetSelection {
                    topic: value.topic.clone(),
                    partition: value.partition,
                })
                .collect(),
        })
        .collect();
    let outcomes = listing
        .groups
        .iter()
        .enumerate()
        .map(|(group_index, group)| AdminShareGroupOffsetsOutcome {
            group_id: group.group_id.clone(),
            error_code: None,
            offsets: group
                .partitions
                .iter()
                .map(|value| AdminShareGroupOffsetOutcome {
                    topic: value.topic.clone(),
                    partition: value.partition,
                    topic_id: [u8::try_from(group_index + 1).unwrap_or(1); 16],
                    start_offset: Some(value.expected_start_offset),
                    leader_epoch: Some(0),
                    lag: Some(value.expected_lag),
                    error_code: None,
                })
                .collect(),
        })
        .collect();
    let mut history = vec![
        command(
            0,
            AdapterCommand::ListShareGroupsOffsets(ListShareGroupsOffsetsCommand {
                client_id: listing.client_id.clone(),
                operation_id: listing.operation_id.clone(),
                groups,
                timeout_ms: listing.timeout_ms,
            }),
        ),
        event(
            1,
            AdapterEvent::ShareGroupsOffsetsListed(AdminShareGroupsOffsetsListing {
                operation_id: listing.operation_id.clone(),
                groups: outcomes,
            }),
        ),
    ];
    for (index, (group_id, value)) in listing
        .groups
        .iter()
        .flat_map(|group| {
            group
                .partitions
                .iter()
                .map(move |value| (&group.group_id, value))
        })
        .enumerate()
    {
        let sequence = u64::try_from(index).unwrap_or(0) + 2;
        history.push(offset_state(
            sequence,
            &listing.operation_id,
            group_id,
            value,
        ));
    }
    history
}

fn violations(action: ScenarioAction, history: &[HistoryEntry]) -> Vec<testlab_schema::Violation> {
    let mut value = scenario(
        TerminalStatus::Acknowledged,
        VisibilityExpectation::ExactlyOnce,
    );
    value.steps.insert(2, step("admin-share-offsets", action));
    let index = HistoryIndex::build(history);
    let mut violations = Vec::new();
    verify_admin(&value, &index, &[], &mut violations);
    violations
}

fn group(group_id: &str, values: &[(&str, i32, i64, i64)]) -> ShareGroupOffsetsExpectation {
    ShareGroupOffsetsExpectation {
        group_id: group_id.to_owned(),
        partitions: values
            .iter()
            .map(|(topic, partition, expected_start_offset, expected_lag)| {
                ShareGroupOffsetExpectation {
                    topic: (*topic).to_owned(),
                    partition: *partition,
                    expected_start_offset: *expected_start_offset,
                    expected_lag: *expected_lag,
                }
            })
            .collect(),
    }
}

fn offset_state(
    sequence: u64,
    operation_id: &OperationId,
    group_id: &str,
    value: &ShareGroupOffsetExpectation,
) -> HistoryEntry {
    HistoryEntry {
        sequence,
        observed_unix_ms: sequence,
        payload: HistoryPayload::BrokerStateObservation {
            observation: BrokerStateObservation::ShareGroupOffset(BrokerShareGroupOffset {
                observation: sequence + 10,
                operation_id: operation_id.clone(),
                group_id: group_id.to_owned(),
                topic: value.topic.clone(),
                partition: value.partition,
                start_offset: Some(value.expected_start_offset),
                lag: Some(value.expected_lag),
            }),
        },
    }
}

fn listed(history: &mut [HistoryEntry]) -> &mut AdminShareGroupsOffsetsListing {
    let HistoryPayload::AdapterEvent { event } = &mut history[1].payload else {
        panic!("plural Share listing event fixture");
    };
    let AdapterEvent::ShareGroupsOffsetsListed(value) = &mut event.event else {
        panic!("plural Share listing outcome fixture");
    };
    value
}

fn offset_observation(entry: &mut HistoryEntry) -> &mut BrokerShareGroupOffset {
    let HistoryPayload::BrokerStateObservation { observation } = &mut entry.payload else {
        panic!("broker-state fixture");
    };
    let BrokerStateObservation::ShareGroupOffset(value) = observation else {
        panic!("Share-group offset fixture");
    };
    value
}

fn assert_contract(violations: &[testlab_schema::Violation]) {
    assert!(
        violations
            .iter()
            .any(|value| value.contract_id.as_str() == "ADMIN-043"),
        "{violations:?}"
    );
}

fn client() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client: {error}"))
}

fn operation() -> OperationId {
    OperationId::new("list-share-groups-offsets")
        .unwrap_or_else(|error| panic!("operation: {error}"))
}
