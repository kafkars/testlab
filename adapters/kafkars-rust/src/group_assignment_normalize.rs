//! Public group assignments become stable Testlab transition identities.

use testlab_schema::{
    ConsumerId, GroupAssignmentTransition, GroupAssignmentTransitionKind, TopicPartitionIdentity,
};

use crate::kafkars_api::ConsumerAssignment;

pub(crate) fn transition(
    consumer_id: &ConsumerId,
    kind: GroupAssignmentTransitionKind,
    assignment: &ConsumerAssignment,
) -> GroupAssignmentTransition {
    GroupAssignmentTransition {
        consumer_id: consumer_id.clone(),
        kind,
        assignment_epoch: assignment.assignment_epoch(),
        partitions: partitions(assignment),
    }
}

pub(crate) fn partitions(assignment: &ConsumerAssignment) -> Vec<TopicPartitionIdentity> {
    assignment
        .partitions()
        .iter()
        .map(|partition| TopicPartitionIdentity {
            topic: partition.topic().to_owned(),
            partition: partition.partition(),
        })
        .collect()
}
