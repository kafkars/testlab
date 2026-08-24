//! Read-only admin validation tests cover bounded query and expectation fields.

use std::collections::{BTreeMap, BTreeSet};

use super::{AdminOffsetPosition, ClientId, OperationId, ScenarioAction};
use crate::admin_action_validation::validate;

#[test]
fn admin_queries_accept_inclusive_collection_bounds_and_reserve_identities() {
    let client_id = client("client-1");
    let clients = BTreeMap::from([(client_id.clone(), false)]);
    let mut operation_ids = BTreeSet::new();
    let mut problems = Vec::new();
    let expected_partitions = (0..10_000).collect::<Vec<_>>();
    let required_topics = (0..32).map(|index| format!("topic-{index}")).collect();

    validate(
        &describe_topic(
            client_id.clone(),
            operation("admin-describe"),
            expected_partitions,
        ),
        &clients,
        &mut operation_ids,
        &mut problems,
    );
    validate(
        &list_topics(
            client_id.clone(),
            operation("admin-topics"),
            required_topics,
        ),
        &clients,
        &mut operation_ids,
        &mut problems,
    );
    validate(
        &list_offsets(client_id, operation("admin-offset"), 0, 0),
        &clients,
        &mut operation_ids,
        &mut problems,
    );

    assert!(problems.is_empty(), "{problems:?}");
    assert_eq!(operation_ids.len(), 3);
}

#[test]
fn describe_topic_rejects_invalid_expected_partitions() {
    let client_id = client("client-1");
    let clients = BTreeMap::from([(client_id.clone(), false)]);
    let mut operation_ids = BTreeSet::new();
    let mut problems = Vec::new();

    for (suffix, expected_partitions) in [
        ("empty", Vec::new()),
        ("large", vec![0; 10_001]),
        ("unordered", vec![1, 0, -1]),
    ] {
        validate(
            &describe_topic(
                client_id.clone(),
                operation(&format!("admin-describe-{suffix}")),
                expected_partitions,
            ),
            &clients,
            &mut operation_ids,
            &mut problems,
        );
    }

    assert_problem(&problems, "expected_partitions must not be empty");
    assert_problem(&problems, "expected_partitions has 10001 entries");
    assert_problem(&problems, "expected_partitions must be nonnegative");
    assert_problem(&problems, "expected_partitions must be sorted and unique");
}

#[test]
fn list_topics_rejects_invalid_required_topics() {
    let client_id = client("client-1");
    let clients = BTreeMap::from([(client_id.clone(), false)]);
    let mut operation_ids = BTreeSet::new();
    let mut problems = Vec::new();

    for (suffix, required_topics) in [
        ("empty", Vec::new()),
        (
            "large",
            (0..33).map(|index| format!("topic-{index}")).collect(),
        ),
        (
            "invalid",
            vec![String::new(), "records".to_owned(), "records".to_owned()],
        ),
    ] {
        validate(
            &list_topics(
                client_id.clone(),
                operation(&format!("admin-topics-{suffix}")),
                required_topics,
            ),
            &clients,
            &mut operation_ids,
            &mut problems,
        );
    }

    assert_problem(&problems, "required_topics must not be empty");
    assert_problem(&problems, "required_topics has 33 entries");
    assert_problem(&problems, "required_topics contains an invalid topic");
    assert_problem(&problems, "required_topics contains a duplicate topic");
}

#[test]
fn list_offsets_rejects_invalid_common_and_offset_fields() {
    let client_id = client("client-1");
    let clients = BTreeMap::from([(client_id.clone(), false)]);
    let mut operation_ids = BTreeSet::new();
    let mut problems = Vec::new();
    let action = ScenarioAction::ListOffsets {
        client_id,
        operation_id: operation("admin-offset-invalid"),
        topic: "a".repeat(250),
        partition: -1,
        position: AdminOffsetPosition::Latest,
        expected_offset: -1,
        timeout_ms: 99,
    };

    validate(&action, &clients, &mut operation_ids, &mut problems);

    assert_problem(&problems, "has invalid topic");
    assert_problem(&problems, "partition must be nonnegative");
    assert_problem(&problems, "expected_offset must be nonnegative");
    assert_problem(&problems, "timeout_ms must be between 100 and 60000");
}

fn describe_topic(
    client_id: ClientId,
    operation_id: OperationId,
    expected_partitions: Vec<i32>,
) -> ScenarioAction {
    ScenarioAction::DescribeTopic {
        client_id,
        operation_id,
        topic: "records".to_owned(),
        expected_partitions,
        timeout_ms: 1_000,
    }
}

fn list_topics(
    client_id: ClientId,
    operation_id: OperationId,
    required_topics: Vec<String>,
) -> ScenarioAction {
    ScenarioAction::ListTopics {
        client_id,
        operation_id,
        include_internal: false,
        required_topics,
        timeout_ms: 1_000,
    }
}

fn list_offsets(
    client_id: ClientId,
    operation_id: OperationId,
    partition: i32,
    expected_offset: i64,
) -> ScenarioAction {
    ScenarioAction::ListOffsets {
        client_id,
        operation_id,
        topic: "records".to_owned(),
        partition,
        position: AdminOffsetPosition::Latest,
        expected_offset,
        timeout_ms: 1_000,
    }
}

fn assert_problem(problems: &[String], expected: &str) {
    assert!(
        problems.iter().any(|problem| problem.contains(expected)),
        "missing {expected:?} in {problems:?}"
    );
}

fn client(value: &str) -> ClientId {
    ClientId::new(value).unwrap_or_else(|error| panic!("client id: {error}"))
}

fn operation(value: &str) -> OperationId {
    OperationId::new(value).unwrap_or_else(|error| panic!("operation id: {error}"))
}
