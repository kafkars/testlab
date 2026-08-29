//! Schema-v17 admin validation tests cover bounded names, lists, offsets, and timeouts.

use std::collections::{BTreeMap, BTreeSet};

use super::{
    AlterConsumerGroupOffsetAction, ClientId, DeleteConsumerGroupAction, DescribeClusterAction,
    DescribeConsumerGroupAction, ListConsumerGroupsAction, ListTopicsAction, OperationId,
    ScenarioAction,
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
            required_topics: vec!["records".to_owned(), "records".to_owned()],
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
        "required_topics must contain unique valid topics",
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

fn clients() -> BTreeMap<ClientId, bool> {
    BTreeMap::from([(client(), false)])
}

fn client() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client id: {error}"))
}

fn operation(value: &str) -> OperationId {
    OperationId::new(value).unwrap_or_else(|error| panic!("operation id: {error}"))
}
