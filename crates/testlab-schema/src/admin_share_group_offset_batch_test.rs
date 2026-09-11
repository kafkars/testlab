//! Plural Share-group offset schemas pin caller order and expectation-free commands.

use std::collections::{BTreeMap, BTreeSet};

use super::{
    AdapterCommand, AdapterEvent, AdminShareGroupOffsetOutcome, AdminShareGroupOffsetsOutcome,
    AdminShareGroupsOffsetsListing, ClientId, ListShareGroupsOffsetsAction,
    ListShareGroupsOffsetsCommand, OperationId, Scenario, ScenarioAction,
    ShareGroupOffsetExpectation, ShareGroupOffsetSelection, ShareGroupOffsetsExpectation,
    ShareGroupOffsetsSelection,
};

#[test]
fn action_command_and_public_completion_round_trip_in_caller_order() {
    round_trip(&ScenarioAction::ListShareGroupsOffsets(action()));
    round_trip(&AdapterCommand::ListShareGroupsOffsets(command()));
    round_trip(&AdapterEvent::ShareGroupsOffsetsListed(completion()));
    assert_eq!(command().groups, selections());
}

#[test]
fn checked_in_plural_offset_scenario_is_valid() {
    scenario()
        .validate()
        .unwrap_or_else(|error| panic!("validate plural Share offsets: {error}"));
}

#[test]
fn validation_rejects_nonplural_duplicate_and_invalid_selections() {
    let clients = BTreeMap::from([(client(), false)]);
    for (label, groups, expected) in [
        (
            "singleton",
            vec![expectation("share-a", "topic-a", 0, 1, 1)],
            "between 2 and 32 entries",
        ),
        (
            "duplicate-group",
            vec![
                expectation("share-a", "topic-a", 0, 1, 1),
                expectation("share-a", "topic-b", 0, 1, 1),
            ],
            "unique valid group ids",
        ),
        (
            "duplicate-partition",
            vec![
                ShareGroupOffsetsExpectation {
                    group_id: "share-a".to_owned(),
                    partitions: vec![offset("topic-a", 0, 1, 1), offset("topic-a", 0, 1, 1)],
                },
                expectation("share-b", "topic-b", 0, 1, 1),
            ],
            "duplicate topic-partition",
        ),
        (
            "invalid-offset",
            vec![
                expectation("share-a", "topic-a", 0, 1, 1),
                expectation("share-b", "", -1, -1, -1),
            ],
            "expected_start_offset must be nonnegative",
        ),
    ] {
        let mut action = action();
        action.operation_id = operation(label);
        action.groups = groups;
        let mut problems = Vec::new();
        crate::admin_action_validation::validate(
            &ScenarioAction::ListShareGroupsOffsets(action),
            &clients,
            &mut BTreeSet::new(),
            &mut problems,
        );
        assert!(
            problems.iter().any(|problem| problem.contains(expected)),
            "{label}: {problems:?}"
        );
    }
}

fn action() -> ListShareGroupsOffsetsAction {
    ListShareGroupsOffsetsAction {
        client_id: client(),
        operation_id: operation("list-share-groups-offsets"),
        groups: vec![
            expectation("share-z", "topic-z", 1, 3, 5),
            expectation("share-a", "topic-a", 0, 2, 7),
        ],
        timeout_ms: 1_000,
    }
}

fn command() -> ListShareGroupsOffsetsCommand {
    ListShareGroupsOffsetsCommand {
        client_id: client(),
        operation_id: operation("list-share-groups-offsets"),
        groups: selections(),
        timeout_ms: 1_000,
    }
}

fn completion() -> AdminShareGroupsOffsetsListing {
    AdminShareGroupsOffsetsListing {
        operation_id: operation("list-share-groups-offsets"),
        groups: action()
            .groups
            .into_iter()
            .map(|group| AdminShareGroupOffsetsOutcome {
                group_id: group.group_id,
                error_code: None,
                offsets: group
                    .partitions
                    .into_iter()
                    .map(|offset| AdminShareGroupOffsetOutcome {
                        topic: offset.topic,
                        partition: offset.partition,
                        topic_id: [1; 16],
                        start_offset: Some(offset.expected_start_offset),
                        leader_epoch: Some(0),
                        lag: Some(offset.expected_lag),
                        error_code: None,
                    })
                    .collect(),
            })
            .collect(),
    }
}

fn selections() -> Vec<ShareGroupOffsetsSelection> {
    action()
        .groups
        .into_iter()
        .map(|group| ShareGroupOffsetsSelection {
            group_id: group.group_id,
            partitions: group
                .partitions
                .into_iter()
                .map(|offset| ShareGroupOffsetSelection {
                    topic: offset.topic,
                    partition: offset.partition,
                })
                .collect(),
        })
        .collect()
}

fn expectation(
    group_id: &str,
    topic: &str,
    partition: i32,
    start_offset: i64,
    lag: i64,
) -> ShareGroupOffsetsExpectation {
    ShareGroupOffsetsExpectation {
        group_id: group_id.to_owned(),
        partitions: vec![offset(topic, partition, start_offset, lag)],
    }
}

fn offset(
    topic: &str,
    partition: i32,
    expected_start_offset: i64,
    expected_lag: i64,
) -> ShareGroupOffsetExpectation {
    ShareGroupOffsetExpectation {
        topic: topic.to_owned(),
        partition,
        expected_start_offset,
        expected_lag,
    }
}

fn scenario() -> Scenario {
    toml::from_str(include_str!(
        "../../../scenarios/kafka/admin-list-share-groups-offsets.toml"
    ))
    .unwrap_or_else(|error| panic!("parse plural Share offsets: {error}"))
}

fn round_trip<T>(value: &T)
where
    T: serde::Serialize + serde::de::DeserializeOwned + std::fmt::Debug + PartialEq,
{
    let encoded = serde_json::to_string(value)
        .unwrap_or_else(|error| panic!("encode plural Share offsets: {error}"));
    let decoded = serde_json::from_str::<T>(&encoded)
        .unwrap_or_else(|error| panic!("decode plural Share offsets: {error}"));
    assert_eq!(&decoded, value);
}

fn client() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client: {error}"))
}

fn operation(value: &str) -> OperationId {
    OperationId::new(value).unwrap_or_else(|error| panic!("operation: {error}"))
}
