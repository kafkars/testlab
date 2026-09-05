//! Receive-time transitions remain ordered, member-scoped, and fail closed at capacity.

use std::collections::BTreeSet;

use testlab_schema::{
    ConsumerId, GroupAssignmentTransition, GroupAssignmentTransitionKind, TopicPartitionIdentity,
};

use super::group_assignment_observe::{record_transition, take_pending_transitions};

#[test]
fn later_observation_retains_order_and_does_not_consume_other_members() {
    let mut pending = Vec::new();
    for (member, epoch) in [("first", 1), ("second", 2), ("first", 3), ("second", 4)] {
        assert!(record_transition(&mut pending, transition(member, epoch)).is_ok());
    }
    let selected = take_pending_transitions(&mut pending, &BTreeSet::from([member("first")]));
    assert_eq!(epochs(&selected), [1, 3]);
    assert_eq!(epochs(&pending), [2, 4]);
    assert!(
        selected
            .iter()
            .all(|event| event.consumer_id == member("first"))
    );
    let selected = take_pending_transitions(&mut pending, &BTreeSet::from([member("second")]));
    assert_eq!(epochs(&selected), [2, 4]);
    assert!(pending.is_empty());
}

#[test]
fn transition_capacity_rejects_growth_without_discarding_retained_facts() {
    let mut pending = Vec::new();
    for epoch in 0..256 {
        assert!(record_transition(&mut pending, transition("first", epoch)).is_ok());
    }
    assert!(record_transition(&mut pending, transition("first", 256)).is_err());
    assert_eq!(pending.len(), 256);
    assert_eq!(pending.first().map(|event| event.assignment_epoch), Some(0));
    assert_eq!(
        pending.last().map(|event| event.assignment_epoch),
        Some(255)
    );
}

#[test]
fn later_observation_preserves_transition_kind_and_partition_order() {
    let mut expected = Vec::new();
    for kind in [
        GroupAssignmentTransitionKind::Assigned,
        GroupAssignmentTransitionKind::Revoking,
        GroupAssignmentTransitionKind::Lost,
    ] {
        let mut event = transition("first", 7);
        event.kind = kind;
        event.partitions = [2, 0]
            .into_iter()
            .map(|partition| TopicPartitionIdentity {
                topic: "orders".to_owned(),
                partition,
            })
            .collect();
        expected.push(event);
    }
    let mut pending = Vec::new();
    for event in &expected {
        assert!(record_transition(&mut pending, event.clone()).is_ok());
    }
    assert_eq!(
        take_pending_transitions(&mut pending, &BTreeSet::from([member("first")])),
        expected
    );
    assert!(pending.is_empty());
}

fn member(name: &str) -> ConsumerId {
    ConsumerId::new(name).unwrap_or_else(|error| panic!("consumer id: {error}"))
}

fn transition(name: &str, epoch: u64) -> GroupAssignmentTransition {
    GroupAssignmentTransition {
        consumer_id: member(name),
        kind: GroupAssignmentTransitionKind::Assigned,
        assignment_epoch: epoch,
        partitions: Vec::new(),
    }
}

fn epochs(transitions: &[GroupAssignmentTransition]) -> Vec<u64> {
    transitions
        .iter()
        .map(|event| event.assignment_epoch)
        .collect()
}
