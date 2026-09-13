//! Topic-partition normalization retains every public topology getter.

use testlab_schema::AdminTopicPartitionDescriptionOutcome;

use crate::kafkars_api::{DescribeTopicPartition, TopicPartitionDescription};
use crate::normalize;

pub(crate) fn metadata(
    partition: &TopicPartitionDescription,
) -> AdminTopicPartitionDescriptionOutcome {
    AdminTopicPartitionDescriptionOutcome {
        partition: partition.partition_index(),
        error_code: partition.error().map(normalize::error_code),
        leader_id: partition.leader_id(),
        leader_epoch: partition.leader_epoch(),
        replicas: partition.replicas().to_vec(),
        in_sync_replicas: partition.in_sync_replicas().to_vec(),
        eligible_leader_replicas: None,
        last_known_eligible_leader_replicas: None,
        offline_replicas: partition.offline_replicas().to_vec(),
    }
}

pub(crate) fn paginated(
    partition: &DescribeTopicPartition,
) -> AdminTopicPartitionDescriptionOutcome {
    AdminTopicPartitionDescriptionOutcome {
        partition: partition.partition_index(),
        error_code: partition.error().map(normalize::error_code),
        leader_id: partition.leader_id(),
        leader_epoch: partition.leader_epoch(),
        replicas: partition.replicas().to_vec(),
        in_sync_replicas: partition.in_sync_replicas().to_vec(),
        eligible_leader_replicas: partition
            .eligible_leader_replicas()
            .map(|replicas| replicas.to_vec()),
        last_known_eligible_leader_replicas: partition
            .last_known_eligible_leader_replicas()
            .map(|replicas| replicas.to_vec()),
        offline_replicas: partition.offline_replicas().to_vec(),
    }
}
