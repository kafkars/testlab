//! Multi-group offset tests preserve nested public and independent order.

use testlab_schema::{
    AdapterCommand, AdapterEvent, AdminConsumerGroupOffsetOutcome,
    AdminConsumerGroupOffsetsOutcome, AdminConsumerGroupsOffsetsListing, BrokerConsumerGroupOffset,
    BrokerStateObservation, ClientId, ConsumerGroupOffsetExpectation, ConsumerGroupOffsetSelection,
    ConsumerGroupOffsetsExpectation, ConsumerGroupOffsetsSelection, HistoryEntry, HistoryPayload,
    ListConsumerGroupsOffsetsAction, ListConsumerGroupsOffsetsCommand, OperationId, ScenarioAction,
    TerminalStatus, VisibilityExpectation,
};

use crate::admin::verify_admin;
use crate::index::HistoryIndex;
use crate::verify_fixture::{command, event, scenario, step};

#[test]
fn exact_nested_multi_group_listing_passes() {
    let action = list_action();
    assert!(violations(vec![action.clone()], &list_history(&action)).is_empty());
}

#[test]
fn multi_group_listing_rejects_public_or_independent_reordering() {
    let action = list_action();
    let mut public = list_history(&action);
    listed(&mut public).groups.swap(0, 1);
    assert_contract(&violations(vec![action.clone()], &public), "ADMIN-024");

    let mut independent = list_history(&action);
    independent.swap(2, 3);
    assert_contract(&violations(vec![action], &independent), "ADMIN-024");
}

#[test]
fn multi_group_listing_rejects_group_or_partition_errors() {
    let action = list_action();
    let mut group_error = list_history(&action);
    listed(&mut group_error).groups[0].error_code = Some("group_authorization_failed".to_owned());
    assert_contract(&violations(vec![action.clone()], &group_error), "ADMIN-024");

    let mut partition_error = list_history(&action);
    listed(&mut partition_error).groups[1].offsets[0].error_code =
        Some("unknown_topic_or_partition".to_owned());
    assert_contract(&violations(vec![action], &partition_error), "ADMIN-024");
}

#[test]
fn multi_group_listing_rejects_wrong_independent_offset() {
    let action = list_action();
    let mut history = list_history(&action);
    offset_observation(&mut history[4]).offset = Some(8);
    assert_contract(&violations(vec![action], &history), "ADMIN-024");
}

pub(crate) fn list_action() -> ScenarioAction {
    ScenarioAction::ListConsumerGroupsOffsets(ListConsumerGroupsOffsetsAction {
        client_id: client(),
        operation_id: operation("list-groups"),
        require_stable: true,
        groups: vec![
            group("group-b", [("topic-b", 1, 4)].as_slice()),
            group("group-a", [("topic-a", 0, 7), ("topic-c", 2, 9)].as_slice()),
        ],
        timeout_ms: 2_000,
    })
}

pub(crate) fn list_history(action: &ScenarioAction) -> Vec<HistoryEntry> {
    let ScenarioAction::ListConsumerGroupsOffsets(listing) = action else {
        panic!("listing action fixture");
    };
    let groups = listing
        .groups
        .iter()
        .map(|group| ConsumerGroupOffsetsSelection {
            group_id: group.group_id.clone(),
            partitions: group
                .partitions
                .iter()
                .map(|value| selection(&value.topic, value.partition))
                .collect(),
        })
        .collect();
    let outcomes = listing
        .groups
        .iter()
        .map(|group| AdminConsumerGroupOffsetsOutcome {
            group_id: group.group_id.clone(),
            error_code: None,
            offsets: group
                .partitions
                .iter()
                .map(|value| outcome(&value.topic, value.partition, value.expected_offset))
                .collect(),
        })
        .collect();
    let mut history = vec![
        command(
            0,
            AdapterCommand::ListConsumerGroupsOffsets(ListConsumerGroupsOffsetsCommand {
                client_id: listing.client_id.clone(),
                operation_id: listing.operation_id.clone(),
                require_stable: listing.require_stable,
                groups,
                timeout_ms: listing.timeout_ms,
            }),
        ),
        event(
            1,
            AdapterEvent::ConsumerGroupsOffsetsListed(AdminConsumerGroupsOffsetsListing {
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
            &value.topic,
            value.partition,
            Some(value.expected_offset),
        ));
    }
    history
}

pub(crate) fn violations(
    actions: Vec<ScenarioAction>,
    history: &[HistoryEntry],
) -> Vec<testlab_schema::Violation> {
    let mut value = scenario(
        TerminalStatus::Acknowledged,
        VisibilityExpectation::ExactlyOnce,
    );
    for (index, action) in actions.into_iter().enumerate() {
        value
            .steps
            .insert(2 + index, step(&format!("admin-{index}"), action));
    }
    let index = HistoryIndex::build(history);
    let mut violations = Vec::new();
    verify_admin(&value, &index, &[], &mut violations);
    violations
}

fn group(group_id: &str, values: &[(&str, i32, i64)]) -> ConsumerGroupOffsetsExpectation {
    ConsumerGroupOffsetsExpectation {
        group_id: group_id.to_owned(),
        partitions: values
            .iter()
            .map(
                |(topic, partition, offset)| ConsumerGroupOffsetExpectation {
                    topic: (*topic).to_owned(),
                    partition: *partition,
                    expected_offset: *offset,
                },
            )
            .collect(),
    }
}

fn selection(topic: &str, partition: i32) -> ConsumerGroupOffsetSelection {
    ConsumerGroupOffsetSelection {
        topic: topic.to_owned(),
        partition,
    }
}

fn outcome(topic: &str, partition: i32, offset: i64) -> AdminConsumerGroupOffsetOutcome {
    AdminConsumerGroupOffsetOutcome {
        topic: topic.to_owned(),
        partition,
        offset: Some(offset),
        error_code: None,
    }
}

fn offset_state(
    sequence: u64,
    operation_id: &OperationId,
    group_id: &str,
    topic: &str,
    partition: i32,
    offset: Option<i64>,
) -> HistoryEntry {
    HistoryEntry {
        sequence,
        observed_unix_ms: sequence,
        payload: HistoryPayload::BrokerStateObservation {
            observation: BrokerStateObservation::ConsumerGroupOffset(BrokerConsumerGroupOffset {
                observation: sequence,
                operation_id: operation_id.clone(),
                group_id: group_id.to_owned(),
                topic: topic.to_owned(),
                partition,
                offset,
            }),
        },
    }
}

fn listed(history: &mut [HistoryEntry]) -> &mut AdminConsumerGroupsOffsetsListing {
    let HistoryPayload::AdapterEvent { event } = &mut history[1].payload else {
        panic!("listing event fixture");
    };
    let AdapterEvent::ConsumerGroupsOffsetsListed(value) = &mut event.event else {
        panic!("listing outcome fixture");
    };
    value
}

pub(crate) fn offset_observation(entry: &mut HistoryEntry) -> &mut BrokerConsumerGroupOffset {
    let HistoryPayload::BrokerStateObservation { observation } = &mut entry.payload else {
        panic!("broker-state fixture");
    };
    let BrokerStateObservation::ConsumerGroupOffset(value) = observation else {
        panic!("group-offset fixture");
    };
    value
}

pub(crate) fn assert_contract(violations: &[testlab_schema::Violation], contract: &str) {
    assert!(
        violations
            .iter()
            .any(|value| value.contract_id.as_str() == contract),
        "{violations:?}"
    );
}

fn client() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client id: {error}"))
}

fn operation(value: &str) -> OperationId {
    OperationId::new(value).unwrap_or_else(|error| panic!("operation id: {error}"))
}
