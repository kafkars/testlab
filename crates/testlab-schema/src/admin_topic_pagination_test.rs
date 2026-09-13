//! Topic pagination validation tests pin explicit API, bound, and page relationships.

use std::collections::{BTreeMap, BTreeSet};

use crate::{
    ClientId, DescribeTopicAction, OperationId, ScenarioAction, TopicDescriptionApi,
    TopicDescriptionPagination,
};

#[test]
fn explicit_pagination_accepts_a_complete_bounded_chain() {
    assert!(problems(&paginated()).is_empty());
}

#[test]
fn description_api_and_pagination_must_agree() {
    let mut metadata = paginated();
    metadata.api = TopicDescriptionApi::Metadata;
    assert_problem(&metadata, "metadata description cannot declare");

    let mut missing = paginated();
    missing.pagination = None;
    assert_problem(&missing, "requires pagination controls");
}

#[test]
fn page_limits_and_follow_intent_are_bounded() {
    let mut invalid_limit = paginated();
    invalid_limit.pagination = Some(TopicDescriptionPagination {
        response_partition_limit: 0,
        follow_cursors: true,
    });
    assert_problem(&invalid_limit, "response_partition_limit must be between");

    let mut no_follow = paginated();
    no_follow.pagination = Some(TopicDescriptionPagination {
        response_partition_limit: 2,
        follow_cursors: false,
    });
    assert_problem(&no_follow, "without follow_cursors requires exactly one");

    let mut one_page = paginated();
    one_page.expected_partitions = Some(vec![0, 1]);
    one_page.expected_page_partitions = Some(vec![vec![0, 1]]);
    assert_problem(&one_page, "follow_cursors requires at least two");
}

#[test]
fn page_boundaries_must_exactly_reconstruct_the_result() {
    let mut detached = paginated();
    detached.expected_page_partitions = Some(vec![vec![0, 1], vec![3, 4]]);
    assert_problem(&detached, "must exactly flatten to expected_partitions");

    let mut short_continued_page = paginated();
    short_continued_page.expected_page_partitions = Some(vec![vec![0], vec![1, 2], vec![3, 4]]);
    assert_problem(
        &short_continued_page,
        "each continued page must fill response_partition_limit",
    );
}

fn paginated() -> DescribeTopicAction {
    DescribeTopicAction {
        client_id: client(),
        operation_id: operation(),
        topic: "orders".to_owned(),
        api: TopicDescriptionApi::DescribeTopicPartitions,
        pagination: Some(TopicDescriptionPagination {
            response_partition_limit: 2,
            follow_cursors: true,
        }),
        expected_partitions: Some(vec![0, 1, 2, 3, 4]),
        expected_page_partitions: Some(vec![vec![0, 1], vec![2, 3], vec![4]]),
        expected_error_code: None,
        timeout_ms: 1_000,
    }
}

fn problems(action: &DescribeTopicAction) -> Vec<String> {
    let client = client();
    let clients = BTreeMap::from([(client, false)]);
    let mut operation_ids = BTreeSet::new();
    let mut problems = Vec::new();
    crate::admin_action_validation::validate(
        &ScenarioAction::DescribeTopic(action.clone()),
        &clients,
        &mut operation_ids,
        &mut problems,
    );
    problems
}

fn assert_problem(action: &DescribeTopicAction, expected: &str) {
    let problems = problems(action);
    assert!(
        problems.iter().any(|problem| problem.contains(expected)),
        "expected {expected:?} in {problems:?}"
    );
}

fn client() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client id: {error}"))
}

fn operation() -> OperationId {
    OperationId::new("describe-pages").unwrap_or_else(|error| panic!("operation id: {error}"))
}
