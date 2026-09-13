//! Schema-v17 admin validation tests cover bounded names, lists, offsets, and timeouts.

use std::collections::{BTreeMap, BTreeSet};

use super::{
    AlterConsumerGroupOffsetAction, ClientId, DeleteConsumerGroupAction, DescribeClusterAction,
    DescribeConsumerGroupAction, ListConsumerGroupsAction, ListTopicsAction, OperationId,
    ScenarioAction, TopicListingExpectation,
};
use crate::admin_action_validation::validate;

#[test]
fn admin_lists_require_unique_valid_resource_names() {
    let clients = clients();
    let mut operation_ids = BTreeSet::new();
    let mut problems = Vec::new();
    validate(
        &ScenarioAction::ListConsumerGroups(ListConsumerGroupsAction {
            client_id: client(),
            operation_id: operation("admin-groups-list"),
            api: crate::GroupListingApi::default(),
            state_filters: Vec::new(),
            group_type_filters: Vec::new(),
            protocol_type_filters: Vec::new(),
            required_group_ids: vec!["group-1".to_owned(), "group-1".to_owned()],
            timeout_ms: 1_000,
        }),
        &clients,
        &mut operation_ids,
        &mut problems,
    );
    validate(
        &ScenarioAction::ListTopics(ListTopicsAction {
            client_id: client(),
            operation_id: operation("admin-topics-list"),
            include_internal: false,
            include_authorized_operations: false,
            expected_topics: vec!["records", "records"]
                .into_iter()
                .map(|topic| TopicListingExpectation {
                    topic: topic.to_owned(),
                    internal: false,
                    included: true,
                })
                .collect(),
            timeout_ms: 1_000,
        }),
        &clients,
        &mut operation_ids,
        &mut problems,
    );

    assert_problem(
        &problems,
        "required_group_ids must contain unique valid groups",
    );
    assert_problem(
        &problems,
        "expected_topics must contain unique valid topics",
    );
}

#[test]
fn group_listing_filters_are_bounded_and_api_specific() {
    let mut action = ListConsumerGroupsAction {
        client_id: client(),
        operation_id: operation("admin-filtered-groups"),
        api: crate::GroupListingApi::default(),
        state_filters: vec!["Stable".to_owned(), "Stable".to_owned()],
        group_type_filters: vec![String::new()],
        protocol_type_filters: vec!["consumer".to_owned()],
        required_group_ids: vec!["group-1".to_owned()],
        timeout_ms: 1_000,
    };
    let mut problems = Vec::new();
    validate(
        &ScenarioAction::ListConsumerGroups(action.clone()),
        &clients(),
        &mut BTreeSet::new(),
        &mut problems,
    );
    for expected in [
        "state_filters must contain unique",
        "group_type_filters must contain unique",
        "protocol_type_filters require api = all_groups",
    ] {
        assert_problem(&problems, expected);
    }

    action.api = crate::GroupListingApi::AllGroups;
    action.state_filters = vec!["Stable".to_owned()];
    action.group_type_filters = vec!["classic".to_owned()];
    assert!(
        problems_for(action).is_empty(),
        "canonical generic filters must validate"
    );
}

#[test]
fn admin_mutations_reject_invalid_names_offsets_and_timeouts() {
    let clients = clients();
    let mut operation_ids = BTreeSet::new();
    let mut problems = Vec::new();
    validate(
        &ScenarioAction::DescribeConsumerGroup(DescribeConsumerGroupAction {
            client_id: client(),
            operation_id: operation("admin-group-invalid"),
            group_id: String::new(),
            expected_member_count: 0,
            include_authorized_operations: false,
            timeout_ms: 99,
        }),
        &clients,
        &mut operation_ids,
        &mut problems,
    );
    validate(
        &ScenarioAction::AlterConsumerGroupOffset(AlterConsumerGroupOffsetAction {
            client_id: client(),
            operation_id: operation("admin-offset-invalid"),
            group_id: "g".repeat(256),
            topic: "t".repeat(250),
            partition: -1,
            offset: -1,
            timeout_ms: 60_001,
        }),
        &clients,
        &mut operation_ids,
        &mut problems,
    );

    for expected in [
        "invalid group_id",
        "invalid topic",
        "partition must be nonnegative",
        "offset must be nonnegative",
        "timeout_ms must be between 100 and 60000",
    ] {
        assert_problem(&problems, expected);
    }
}

#[test]
fn admin_validation_accepts_inclusive_name_offset_and_timeout_bounds() {
    let clients = clients();
    let mut operation_ids = BTreeSet::new();
    let mut problems = Vec::new();
    for action in [
        ScenarioAction::DescribeCluster(DescribeClusterAction {
            client_id: client(),
            operation_id: operation("admin-cluster-min"),
            include_fenced_brokers: false,
            include_authorized_operations: false,
            expected_fenced_broker_ids: Vec::new(),
            timeout_ms: 100,
        }),
        ScenarioAction::AlterConsumerGroupOffset(AlterConsumerGroupOffsetAction {
            client_id: client(),
            operation_id: operation("admin-offset-max"),
            group_id: "g".repeat(255),
            topic: "t".repeat(249),
            partition: 0,
            offset: 0,
            timeout_ms: 60_000,
        }),
        ScenarioAction::DeleteConsumerGroup(DeleteConsumerGroupAction {
            client_id: client(),
            operation_id: operation("admin-group-delete"),
            group_id: "g".repeat(255),
            timeout_ms: 100,
        }),
    ] {
        validate(&action, &clients, &mut operation_ids, &mut problems);
    }

    assert!(problems.is_empty(), "{problems:?}");
}

fn assert_problem(problems: &[String], expected: &str) {
    assert!(
        problems.iter().any(|problem| problem.contains(expected)),
        "missing {expected:?} in {problems:?}"
    );
}

fn problems_for(action: ListConsumerGroupsAction) -> Vec<String> {
    let mut problems = Vec::new();
    validate(
        &ScenarioAction::ListConsumerGroups(action),
        &clients(),
        &mut BTreeSet::new(),
        &mut problems,
    );
    problems
}

fn clients() -> BTreeMap<ClientId, bool> {
    BTreeMap::from([(client(), false)])
}

fn client() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client id: {error}"))
}

fn operation(value: &str) -> OperationId {
    OperationId::new(value).unwrap_or_else(|error| panic!("operation id: {error}"))
}
