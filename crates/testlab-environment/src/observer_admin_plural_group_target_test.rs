//! Plural group-admin targets pin exact wire order, identities, and observation sizing.

use testlab_schema::{
    AdapterCommand, AlterConsumerGroupOffsetsAction, ClassicGroupExpectation, ClientId,
    ConsumerGroupOffsetAlteration, ConsumerGroupOffsetExpectation, ConsumerGroupOffsetSelection,
    ConsumerGroupOffsetsExpectation, DeleteConsumerGroupOffsetsAction, DescribeClassicGroupsAction,
    ListConsumerGroupOffsetsBatchAction, ListConsumerGroupsOffsetsAction, OperationId,
    ScenarioAction,
};

use crate::compose_test_fixture::Fixture;
use crate::observer_admin_target::AdminTarget;

#[test]
fn one_group_batch_preserves_partition_order_and_exact_wire_selection() {
    let action =
        ScenarioAction::ListConsumerGroupOffsetsBatch(ListConsumerGroupOffsetsBatchAction {
            client_id: client(),
            operation_id: operation("list-offsets"),
            group_id: "orders-group".to_owned(),
            require_stable: true,
            partitions: expectations(),
            timeout_ms: 500,
        });
    let (command, target) = matched(&action);
    let AdminTarget::ConsumerGroupOffsets(target) = &target else {
        panic!("one-group offset target kind");
    };
    assert_eq!(target.group_id, "orders-group");
    assert_eq!(target.offsets[0].topic, "orders-b");
    assert_eq!(target.offsets[1].topic, "orders-a");
    assert_eq!(target.offsets[0].expected_offset, Some(8));
    assert!(!target.poll_expected);
    assert_eq!(target_observation_count(target), 2);

    let AdapterCommand::ListConsumerGroupOffsetsBatch(mut mismatched) = command else {
        panic!("list offsets command kind");
    };
    mismatched.partitions.swap(0, 1);
    assert!(
        AdminTarget::from_exact(
            &action,
            &AdapterCommand::ListConsumerGroupOffsetsBatch(mismatched),
        )
        .is_err()
    );
}

#[test]
fn multi_group_listing_retains_group_then_partition_order() {
    let action = ScenarioAction::ListConsumerGroupsOffsets(ListConsumerGroupsOffsetsAction {
        client_id: client(),
        operation_id: operation("list-groups-offsets"),
        require_stable: false,
        groups: vec![
            ConsumerGroupOffsetsExpectation {
                group_id: "group-b".to_owned(),
                partitions: expectations(),
            },
            ConsumerGroupOffsetsExpectation {
                group_id: "group-a".to_owned(),
                partitions: vec![expectation("audit", 3, 12)],
            },
        ],
        timeout_ms: 500,
    });
    let (command, target) = matched(&action);
    let AdminTarget::ConsumerGroupsOffsets(target) = &target else {
        panic!("multi-group offset target kind");
    };
    assert_eq!(target.groups[0].group_id, "group-b");
    assert_eq!(target.groups[1].group_id, "group-a");
    assert_eq!(target.groups[0].offsets[0].topic, "orders-b");
    assert_eq!(
        AdminTarget::ConsumerGroupsOffsets(target.clone()).observation_count(),
        3
    );

    let AdapterCommand::ListConsumerGroupsOffsets(mut mismatched) = command else {
        panic!("multi-group offsets command kind");
    };
    mismatched.groups.swap(0, 1);
    assert!(
        AdminTarget::from_exact(
            &action,
            &AdapterCommand::ListConsumerGroupsOffsets(mismatched),
        )
        .is_err()
    );
}

#[test]
fn plural_mutations_poll_the_complete_requested_offsets() {
    let alter = ScenarioAction::AlterConsumerGroupOffsets(AlterConsumerGroupOffsetsAction {
        client_id: client(),
        operation_id: operation("alter-offsets"),
        group_id: "orders-group".to_owned(),
        offsets: vec![alteration("orders-b", 2, 15), alteration("orders-a", 0, 9)],
        timeout_ms: 500,
    });
    let AdminTarget::ConsumerGroupOffsets(alter) = matched(&alter).1 else {
        panic!("alter offsets target kind");
    };
    assert!(alter.poll_expected);
    assert_eq!(alter.offsets[0].expected_offset, Some(15));
    assert_eq!(alter.offsets[1].expected_offset, Some(9));

    let delete = ScenarioAction::DeleteConsumerGroupOffsets(DeleteConsumerGroupOffsetsAction {
        client_id: client(),
        operation_id: operation("delete-offsets"),
        group_id: "orders-group".to_owned(),
        partitions: vec![selection("orders-b", 2), selection("orders-a", 0)],
        timeout_ms: 500,
    });
    let AdminTarget::ConsumerGroupOffsets(delete) = matched(&delete).1 else {
        panic!("delete offsets target kind");
    };
    assert!(delete.poll_expected);
    assert!(
        delete
            .offsets
            .iter()
            .all(|item| item.expected_offset.is_none())
    );
}

#[test]
fn classic_group_description_preserves_caller_order_and_reserves_every_fact() {
    let action = ScenarioAction::DescribeClassicGroups(DescribeClassicGroupsAction {
        client_id: client(),
        operation_id: operation("describe-groups"),
        groups: vec![classic("group-b", 2), classic("group-a", 0)],
        timeout_ms: 500,
    });
    let (command, target) = matched(&action);
    let AdminTarget::ClassicGroups(target) = &target else {
        panic!("classic groups target kind");
    };
    assert_eq!(target.group_ids, ["group-b", "group-a"]);
    assert_eq!(
        AdminTarget::ClassicGroups(target.clone()).observation_count(),
        2
    );

    let AdapterCommand::DescribeClassicGroups(mut mismatched) = command else {
        panic!("classic groups command kind");
    };
    mismatched.group_ids.swap(0, 1);
    assert!(
        AdminTarget::from_exact(&action, &AdapterCommand::DescribeClassicGroups(mismatched),)
            .is_err()
    );
}

#[test]
fn duplicate_resources_and_duplicate_operation_observation_are_rejected() {
    let action =
        ScenarioAction::ListConsumerGroupOffsetsBatch(ListConsumerGroupOffsetsBatchAction {
            client_id: client(),
            operation_id: operation("list-offsets"),
            group_id: "orders-group".to_owned(),
            require_stable: true,
            partitions: vec![expectation("orders", 0, 1), expectation("orders", 0, 1)],
            timeout_ms: 500,
        });
    let command = AdapterCommand::ListConsumerGroupOffsetsBatch(
        testlab_schema::ListConsumerGroupOffsetsBatchCommand {
            client_id: client(),
            operation_id: operation("list-offsets"),
            group_id: "orders-group".to_owned(),
            require_stable: true,
            partitions: vec![selection("orders", 0), selection("orders", 0)],
            timeout_ms: 500,
        },
    );
    assert!(AdminTarget::from_exact(&action, &command).is_err());

    let valid_action =
        ScenarioAction::ListConsumerGroupOffsetsBatch(ListConsumerGroupOffsetsBatchAction {
            client_id: client(),
            operation_id: operation("valid-list"),
            group_id: "orders-group".to_owned(),
            require_stable: true,
            partitions: expectations(),
            timeout_ms: 500,
        });
    let target = matched(&valid_action).1;
    let mut environment = Fixture::new(false).environment();
    assert_eq!(environment.begin_admin_observation(&target).ok(), Some(0));
    assert!(environment.begin_admin_observation(&target).is_err());
}

fn matched(action: &ScenarioAction) -> (AdapterCommand, AdminTarget) {
    crate::observer_admin_plural_group_target::match_action(action)
        .ok()
        .flatten()
        .unwrap_or_else(|| panic!("plural group action must match"))
}

fn target_observation_count(target: &crate::observer_admin_target::GroupOffsetsTarget) -> usize {
    AdminTarget::ConsumerGroupOffsets(target.clone()).observation_count()
}

fn expectations() -> Vec<ConsumerGroupOffsetExpectation> {
    vec![expectation("orders-b", 2, 8), expectation("orders-a", 0, 5)]
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

fn classic(group_id: &str, expected_member_count: u32) -> ClassicGroupExpectation {
    ClassicGroupExpectation {
        group_id: group_id.to_owned(),
        expected_member_count,
    }
}

fn client() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client ID: {error}"))
}

fn operation(value: &str) -> OperationId {
    OperationId::new(value).unwrap_or_else(|error| panic!("operation ID: {error}"))
}
