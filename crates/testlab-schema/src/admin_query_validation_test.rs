//! Read-only admin validation tests cover bounded query and expectation fields.

use std::collections::{BTreeMap, BTreeSet};

use super::{
    AdminOffsetPosition, ClientId, DescribeTopicAction, ListOffsetsAction, ListTopicsAction,
    OperationId, ScenarioAction, UNKNOWN_TOPIC_OR_PARTITION_ERROR_CODE,
};
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

    assert_problem(
        &problems,
        "expected_partitions must contain 1 to 10000 entries",
    );
    assert_problem(
        &problems,
        "expected_partitions must be sorted unique nonnegative indices",
    );
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

    assert_problem(&problems, "required_topics must contain 1 to 32 entries");
    assert_problem(
        &problems,
        "required_topics must contain unique valid topics",
    );
}

#[test]
fn list_offsets_rejects_invalid_common_and_offset_fields() {
    let client_id = client("client-1");
    let clients = BTreeMap::from([(client_id.clone(), false)]);
    let mut operation_ids = BTreeSet::new();
    let mut problems = Vec::new();
    let action = ScenarioAction::ListOffsets(ListOffsetsAction {
        client_id,
        operation_id: operation("admin-offset-invalid"),
        topic: "a".repeat(250),
        partition: -1,
        position: AdminOffsetPosition::Latest,
        expected_offset: Some(-1),
        expected_error_code: None,
        timeout_ms: 99,
    });

    validate(&action, &clients, &mut operation_ids, &mut problems);

    assert_problem(&problems, "has invalid topic");
    assert_problem(&problems, "partition must be nonnegative");
    assert_problem(&problems, "expected_offset must be nonnegative");
    assert_problem(&problems, "timeout_ms must be between 100 and 60000");
}

#[test]
fn query_expectations_require_exactly_one_result_or_error() {
    let client_id = client("client-1");
    let clients = BTreeMap::from([(client_id.clone(), false)]);
    let mut operation_ids = BTreeSet::new();
    let mut problems = Vec::new();
    let actions = [
        ScenarioAction::DescribeTopic(DescribeTopicAction {
            client_id: client_id.clone(),
            operation_id: operation("admin-describe-both"),
            topic: "records".to_owned(),
            expected_partitions: Some(vec![0]),
            expected_error_code: Some(UNKNOWN_TOPIC_OR_PARTITION_ERROR_CODE.to_owned()),
            timeout_ms: 1_000,
        }),
        ScenarioAction::ListOffsets(ListOffsetsAction {
            client_id,
            operation_id: operation("admin-offset-neither"),
            topic: "records".to_owned(),
            partition: 1,
            position: AdminOffsetPosition::Latest,
            expected_offset: None,
            expected_error_code: None,
            timeout_ms: 1_000,
        }),
    ];

    for action in &actions {
        validate(action, &clients, &mut operation_ids, &mut problems);
    }
    assert_eq!(
        problems
            .iter()
            .filter(|problem| problem.contains("must declare exactly one"))
            .count(),
        actions.len(),
        "{problems:?}"
    );
}

#[test]
fn missing_offset_partition_requires_positive_index_and_exact_code() {
    let clients = BTreeMap::from([(client("client-1"), false)]);
    let mut operation_ids = BTreeSet::new();
    let mut problems = Vec::new();
    for (operation_id, partition, code) in [
        (
            "admin-offset-valid",
            1,
            UNKNOWN_TOPIC_OR_PARTITION_ERROR_CODE,
        ),
        (
            "admin-offset-zero",
            0,
            UNKNOWN_TOPIC_OR_PARTITION_ERROR_CODE,
        ),
        ("admin-offset-wrong-code", 2, "broker:broker_36"),
    ] {
        let action = ScenarioAction::ListOffsets(ListOffsetsAction {
            client_id: client("client-1"),
            operation_id: operation(operation_id),
            topic: "records".to_owned(),
            partition,
            position: AdminOffsetPosition::Latest,
            expected_offset: None,
            expected_error_code: Some(code.to_owned()),
            timeout_ms: 1_000,
        });
        validate(&action, &clients, &mut operation_ids, &mut problems);
    }

    assert_problem(
        &problems,
        "expected missing partition must query a positive partition",
    );
    assert_problem(&problems, UNKNOWN_TOPIC_OR_PARTITION_ERROR_CODE);
    assert_eq!(problems.len(), 2, "{problems:?}");
}

fn describe_topic(
    client_id: ClientId,
    operation_id: OperationId,
    expected_partitions: Vec<i32>,
) -> ScenarioAction {
    ScenarioAction::DescribeTopic(DescribeTopicAction {
        client_id,
        operation_id,
        topic: "records".to_owned(),
        expected_partitions: Some(expected_partitions),
        expected_error_code: None,
        timeout_ms: 1_000,
    })
}

fn list_topics(
    client_id: ClientId,
    operation_id: OperationId,
    required_topics: Vec<String>,
) -> ScenarioAction {
    ScenarioAction::ListTopics(ListTopicsAction {
        client_id,
        operation_id,
        include_internal: false,
        required_topics,
        timeout_ms: 1_000,
    })
}

fn list_offsets(
    client_id: ClientId,
    operation_id: OperationId,
    partition: i32,
    expected_offset: i64,
) -> ScenarioAction {
    ScenarioAction::ListOffsets(ListOffsetsAction {
        client_id,
        operation_id,
        topic: "records".to_owned(),
        partition,
        position: AdminOffsetPosition::Latest,
        expected_offset: Some(expected_offset),
        expected_error_code: None,
        timeout_ms: 1_000,
    })
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
