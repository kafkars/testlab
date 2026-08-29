//! Group target tests pin exact expected state after every supported group operation.

use testlab_schema::{
    AlterConsumerGroupOffsetAction, ClientId, DeleteConsumerGroupAction,
    DeleteConsumerGroupOffsetAction, DescribeClusterAction, DescribeConsumerGroupAction,
    ListConsumerGroupOffsetsAction, ListConsumerGroupsAction, OperationId, ScenarioAction,
};

use crate::observer_admin_target::AdminTarget;

#[test]
fn cluster_and_group_discovery_targets_are_exact() {
    let cluster = ScenarioAction::DescribeCluster(DescribeClusterAction {
        client_id: client(),
        operation_id: operation("describe-cluster"),
        timeout_ms: 500,
    });
    assert!(matches!(exact(&cluster), AdminTarget::Cluster(_)));

    let list = ScenarioAction::ListConsumerGroups(ListConsumerGroupsAction {
        client_id: client(),
        operation_id: operation("list-groups"),
        required_group_ids: vec!["orders-group".to_owned()],
        timeout_ms: 500,
    });
    let AdminTarget::ConsumerGroups(target) = exact(&list) else {
        panic!("list groups target kind");
    };
    assert_eq!(target.names, ["orders-group"]);

    let describe = ScenarioAction::DescribeConsumerGroup(DescribeConsumerGroupAction {
        client_id: client(),
        operation_id: operation("describe-group"),
        group_id: "orders-group".to_owned(),
        expected_member_count: 2,
        timeout_ms: 500,
    });
    let AdminTarget::ConsumerGroup(target) = exact(&describe) else {
        panic!("describe group target kind");
    };
    assert!(target.expected_exists);
    assert_eq!(target.expected_member_count, Some(2));
    assert!(!target.poll_expected);
}

#[test]
fn group_offset_targets_distinguish_read_alter_and_delete() {
    let list = ScenarioAction::ListConsumerGroupOffsets(ListConsumerGroupOffsetsAction {
        client_id: client(),
        operation_id: operation("list-offset"),
        group_id: "orders-group".to_owned(),
        topic: "orders".to_owned(),
        partition: 1,
        require_stable: true,
        expected_offset: 7,
        timeout_ms: 500,
    });
    assert_offset(exact(&list), Some(7), false);

    let alter = ScenarioAction::AlterConsumerGroupOffset(AlterConsumerGroupOffsetAction {
        client_id: client(),
        operation_id: operation("alter-offset"),
        group_id: "orders-group".to_owned(),
        topic: "orders".to_owned(),
        partition: 1,
        offset: 9,
        timeout_ms: 500,
    });
    assert_offset(exact(&alter), Some(9), true);

    let delete = ScenarioAction::DeleteConsumerGroupOffset(DeleteConsumerGroupOffsetAction {
        client_id: client(),
        operation_id: operation("delete-offset"),
        group_id: "orders-group".to_owned(),
        topic: "orders".to_owned(),
        partition: 1,
        timeout_ms: 500,
    });
    assert_offset(exact(&delete), None, true);
}

#[test]
fn group_deletion_polls_for_independent_absence() {
    let action = ScenarioAction::DeleteConsumerGroup(DeleteConsumerGroupAction {
        client_id: client(),
        operation_id: operation("delete-group"),
        group_id: "orders-group".to_owned(),
        timeout_ms: 500,
    });
    let AdminTarget::ConsumerGroup(target) = exact(&action) else {
        panic!("delete group target kind");
    };
    assert!(!target.expected_exists);
    assert_eq!(target.expected_member_count, None);
    assert!(target.poll_expected);
}

#[test]
fn duplicate_group_listing_targets_are_rejected() {
    let action = ScenarioAction::ListConsumerGroups(ListConsumerGroupsAction {
        client_id: client(),
        operation_id: operation("list-groups"),
        required_group_ids: vec!["orders-group".to_owned(), "orders-group".to_owned()],
        timeout_ms: 500,
    });
    let (command, _) = crate::observer_admin_group_target::match_action(
        &ScenarioAction::DescribeCluster(DescribeClusterAction {
            client_id: client(),
            operation_id: operation("describe-cluster"),
            timeout_ms: 500,
        }),
    )
    .ok()
    .flatten()
    .unwrap_or_else(|| panic!("cluster target"));

    assert!(AdminTarget::from_exact(&action, &command).is_err());
}

fn assert_offset(target: AdminTarget, expected: Option<i64>, poll: bool) {
    let AdminTarget::ConsumerGroupOffset(target) = target else {
        panic!("group offset target kind");
    };
    assert_eq!(target.group_id, "orders-group");
    assert_eq!(target.topic, "orders");
    assert_eq!(target.partition, 1);
    assert_eq!(target.expected_offset, expected);
    assert_eq!(target.poll_expected, poll);
}

fn exact(action: &ScenarioAction) -> AdminTarget {
    let (command, target) = crate::observer_admin_group_target::match_action(action)
        .ok()
        .flatten()
        .unwrap_or_else(|| panic!("group action must match"));
    assert_eq!(
        AdminTarget::from_exact(action, &command).ok().flatten(),
        Some(target.clone())
    );
    target
}

fn client() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client ID: {error}"))
}

fn operation(value: &str) -> OperationId {
    OperationId::new(value).unwrap_or_else(|error| panic!("operation ID: {error}"))
}
