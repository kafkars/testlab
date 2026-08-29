//! Plural group-admin action tests enforce bounded unique exact resources.

use std::collections::{BTreeMap, BTreeSet};

use super::{
    AlterConsumerGroupOffsetsAction, ClassicGroupExpectation, ClientId,
    ConsumerGroupOffsetAlteration, ConsumerGroupOffsetExpectation, ConsumerGroupOffsetSelection,
    ConsumerGroupOffsetsExpectation, DeleteConsumerGroupOffsetsAction, DescribeClassicGroupsAction,
    ListConsumerGroupOffsetsBatchAction, ListConsumerGroupsOffsetsAction, OperationId,
    ScenarioAction,
};

#[test]
fn all_plural_actions_accept_inclusive_item_bounds_and_stable_ownership() {
    let client_id = client();
    let clients = BTreeMap::from([(client_id.clone(), false)]);
    let mut operation_ids = BTreeSet::new();
    let mut problems = Vec::new();
    let expectations = (0..32)
        .map(|partition| expectation("records", partition, i64::from(partition)))
        .collect::<Vec<_>>();
    let selections = expectations
        .iter()
        .map(|item| selection(&item.topic, item.partition))
        .collect::<Vec<_>>();
    let alterations = expectations
        .iter()
        .map(|item| alteration(&item.topic, item.partition, item.expected_offset))
        .collect::<Vec<_>>();
    let groups = (0..32)
        .map(|index| ConsumerGroupOffsetsExpectation {
            group_id: format!("group-{index}"),
            partitions: vec![expectation("records", index, i64::from(index))],
        })
        .collect::<Vec<_>>();
    let classic_groups = (0..32)
        .map(|index| ClassicGroupExpectation {
            group_id: format!("classic-{index}"),
            expected_member_count: 0,
        })
        .collect::<Vec<_>>();
    let actions = [
        ScenarioAction::ListConsumerGroupOffsetsBatch(ListConsumerGroupOffsetsBatchAction {
            client_id: client_id.clone(),
            operation_id: operation("list-batch"),
            group_id: "g".repeat(255),
            require_stable: true,
            partitions: expectations,
            timeout_ms: 100,
        }),
        ScenarioAction::ListConsumerGroupsOffsets(ListConsumerGroupsOffsetsAction {
            client_id: client_id.clone(),
            operation_id: operation("list-groups"),
            require_stable: false,
            groups,
            timeout_ms: 60_000,
        }),
        ScenarioAction::AlterConsumerGroupOffsets(AlterConsumerGroupOffsetsAction {
            client_id: client_id.clone(),
            operation_id: operation("alter-batch"),
            group_id: "group-1".to_owned(),
            offsets: alterations,
            timeout_ms: 1_000,
        }),
        ScenarioAction::DeleteConsumerGroupOffsets(DeleteConsumerGroupOffsetsAction {
            client_id: client_id.clone(),
            operation_id: operation("delete-batch"),
            group_id: "group-1".to_owned(),
            partitions: selections,
            timeout_ms: 1_000,
        }),
        ScenarioAction::DescribeClassicGroups(DescribeClassicGroupsAction {
            client_id,
            operation_id: operation("describe-classic"),
            groups: classic_groups,
            timeout_ms: 1_000,
        }),
    ];

    for action in &actions {
        crate::admin_action_validation::validate(
            action,
            &clients,
            &mut operation_ids,
            &mut problems,
        );
    }

    assert!(problems.is_empty(), "{problems:?}");
    assert_eq!(operation_ids.len(), actions.len());
}

#[test]
fn plural_actions_reject_empty_oversized_duplicate_and_invalid_items() {
    let clients = BTreeMap::from([(client(), false)]);
    let mut operation_ids = BTreeSet::new();
    let mut problems = Vec::new();
    let actions = [
        ScenarioAction::ListConsumerGroupOffsetsBatch(ListConsumerGroupOffsetsBatchAction {
            client_id: client(),
            operation_id: operation("empty-list"),
            group_id: String::new(),
            require_stable: true,
            partitions: Vec::new(),
            timeout_ms: 1_000,
        }),
        ScenarioAction::ListConsumerGroupsOffsets(ListConsumerGroupsOffsetsAction {
            client_id: client(),
            operation_id: operation("bad-groups"),
            require_stable: true,
            groups: vec![
                ConsumerGroupOffsetsExpectation {
                    group_id: "group-1".to_owned(),
                    partitions: Vec::new(),
                },
                ConsumerGroupOffsetsExpectation {
                    group_id: "group-1".to_owned(),
                    partitions: vec![expectation(&"t".repeat(250), -1, -1)],
                },
            ],
            timeout_ms: 1_000,
        }),
        ScenarioAction::AlterConsumerGroupOffsets(AlterConsumerGroupOffsetsAction {
            client_id: client(),
            operation_id: operation("bad-alter"),
            group_id: "group-1".to_owned(),
            offsets: vec![alteration("records", 0, -1), alteration("records", 0, 1)],
            timeout_ms: 1_000,
        }),
        ScenarioAction::DeleteConsumerGroupOffsets(DeleteConsumerGroupOffsetsAction {
            client_id: client(),
            operation_id: operation("large-delete"),
            group_id: "group-1".to_owned(),
            partitions: (0..33)
                .map(|partition| selection("records", partition))
                .collect(),
            timeout_ms: 1_000,
        }),
        ScenarioAction::DescribeClassicGroups(DescribeClassicGroupsAction {
            client_id: client(),
            operation_id: operation("duplicate-classic"),
            groups: vec![classic("group-1"), classic("group-1")],
            timeout_ms: 1_000,
        }),
    ];

    for action in &actions {
        crate::admin_action_validation::validate(
            action,
            &clients,
            &mut operation_ids,
            &mut problems,
        );
    }

    for expected in [
        "has invalid group_id",
        "partitions must contain 1 to 32 entries",
        "groups must contain unique valid group ids",
        "has invalid topic",
        "partition must be nonnegative",
        "expected_offset must be nonnegative",
        "offset must be nonnegative",
        "contains duplicate topic-partition",
        "partitions must contain 1 to 32 entries",
    ] {
        assert_problem(&problems, expected);
    }
}

#[test]
fn identity_and_timeout_rules_apply_once_to_plural_operations() {
    let duplicate = operation("duplicate");
    let clients = BTreeMap::from([(client(), true)]);
    let mut operation_ids = BTreeSet::from([duplicate]);
    let mut problems = Vec::new();
    let action = ScenarioAction::DescribeClassicGroups(DescribeClassicGroupsAction {
        client_id: client(),
        operation_id: operation("duplicate"),
        groups: vec![classic("group-1")],
        timeout_ms: 99,
    });

    crate::admin_action_validation::validate(&action, &clients, &mut operation_ids, &mut problems);

    assert_problem(&problems, "uses shut down client");
    assert_problem(&problems, "duplicate operation id");
    assert_problem(&problems, "timeout_ms must be between 100 and 60000");
    assert_eq!(
        problems
            .iter()
            .filter(|problem| problem.contains("duplicate operation id"))
            .count(),
        1
    );
}

fn expectation(
    topic: &str,
    partition: i32,
    expected_offset: i64,
) -> ConsumerGroupOffsetExpectation {
    ConsumerGroupOffsetExpectation {
        topic: topic.to_owned(),
        partition,
        expected_offset,
    }
}

fn selection(topic: &str, partition: i32) -> ConsumerGroupOffsetSelection {
    ConsumerGroupOffsetSelection {
        topic: topic.to_owned(),
        partition,
    }
}

fn alteration(topic: &str, partition: i32, offset: i64) -> ConsumerGroupOffsetAlteration {
    ConsumerGroupOffsetAlteration {
        topic: topic.to_owned(),
        partition,
        offset,
    }
}

fn classic(group_id: &str) -> ClassicGroupExpectation {
    ClassicGroupExpectation {
        group_id: group_id.to_owned(),
        expected_member_count: 0,
    }
}

fn assert_problem(problems: &[String], expected: &str) {
    assert!(
        problems.iter().any(|problem| problem.contains(expected)),
        "missing {expected:?} in {problems:?}"
    );
}

fn client() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client id: {error}"))
}

fn operation(value: &str) -> OperationId {
    OperationId::new(value).unwrap_or_else(|error| panic!("operation id: {error}"))
}
