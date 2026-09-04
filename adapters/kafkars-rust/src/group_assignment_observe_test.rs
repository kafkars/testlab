//! Stable group-assignment candidate checks for provisional and settled ownership.

use testlab_schema::{
    ConsumerId, GroupConsumerAssignment, GroupMembershipEpoch, TopicPartitionIdentity,
};

use super::group_assignment_observe::{
    membership_changed, stable_assignment_candidate, superseded_revocation,
};
use super::kafkars_api::ErrorKind;

#[test]
fn stale_revocation_requires_a_strictly_newer_public_assignment() {
    assert!(superseded_revocation(ErrorKind::State, 2, Some(3)));
    for current in [None, Some(0), Some(1), Some(2)] {
        assert!(!superseded_revocation(ErrorKind::State, 2, current));
    }
    for kind in [
        ErrorKind::Internal,
        ErrorKind::Timeout,
        ErrorKind::Backpressure,
    ] {
        assert!(!superseded_revocation(kind, 2, Some(3)));
    }
}

#[test]
fn unchanged_membership_does_not_require_a_new_rebalance_event() {
    let member = |name| ConsumerId::new(name).unwrap_or_else(|error| panic!("member: {error}"));
    let previous = std::collections::BTreeSet::from([member("consumer-1"), member("consumer-2")]);
    let unchanged = std::collections::BTreeSet::from([member("consumer-2"), member("consumer-1")]);
    assert!(!membership_changed(Some(&previous), &unchanged));
    assert!(membership_changed(None, &unchanged));
    assert!(membership_changed(
        Some(&previous),
        &std::collections::BTreeSet::from([member("consumer-1")])
    ));
}

#[test]
fn stable_candidate_requires_nonempty_disjoint_member_ownership() {
    assert!(!stable_assignment_candidate(&[
        assignment("consumer-1", &[("orders", 0)]),
        assignment("consumer-2", &[]),
    ]));
    assert!(!stable_assignment_candidate(&[
        assignment("consumer-1", &[("orders", 0)]),
        assignment("consumer-2", &[("orders", 0)]),
    ]));
    assert!(stable_assignment_candidate(&[
        assignment("consumer-1", &[("orders", 0), ("orders", 1)]),
        assignment("consumer-2", &[("orders", 2), ("orders", 3)]),
    ]));
}

fn assignment(consumer_id: &str, partitions: &[(&str, i32)]) -> GroupConsumerAssignment {
    GroupConsumerAssignment {
        consumer_id: ConsumerId::new(consumer_id)
            .unwrap_or_else(|error| panic!("consumer id: {error}")),
        group_id: "orders-group".to_owned(),
        member_id: format!("member-{consumer_id}"),
        group_epoch: GroupMembershipEpoch::Classic { generation_id: 3 },
        assignment_epoch: 1,
        partitions: partitions
            .iter()
            .map(|(topic, partition)| TopicPartitionIdentity {
                topic: (*topic).to_owned(),
                partition: *partition,
            })
            .collect(),
    }
}
