//! Stable group-assignment candidate checks for provisional and settled ownership.

use testlab_schema::{
    ConsumerId, GroupConsumerAssignment, GroupMembershipEpoch, TopicPartitionIdentity,
};

use super::group_assignment_observe::stable_assignment_candidate;

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
