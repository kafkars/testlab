//! Admin family recognition prevents unrelated events from completing a command.

use testlab_schema::AdapterEvent;

use crate::runner_protocol::ExpectedEvent;

pub(super) fn same_admin_event_family(expected: &ExpectedEvent, event: &AdapterEvent) -> bool {
    expected_is_admin(expected) && event_is_admin(event)
}

fn expected_is_admin(expected: &ExpectedEvent) -> bool {
    matches!(
        expected,
        ExpectedEvent::TopicCreated { .. }
            | ExpectedEvent::TopicCreationValidated { .. }
            | ExpectedEvent::TopicsCreationCompleted { .. }
            | ExpectedEvent::TopicPartitionsCreated { .. }
            | ExpectedEvent::TopicPartitionIncreaseValidated { .. }
            | ExpectedEvent::TopicDeleted { .. }
            | ExpectedEvent::TopicsDeleted { .. }
            | ExpectedEvent::TopicDescribed { .. }
            | ExpectedEvent::TopicsDescribed { .. }
            | ExpectedEvent::TopicsListed { .. }
            | ExpectedEvent::ConfigResourcesListed(_)
            | ExpectedEvent::OffsetListed { .. }
            | ExpectedEvent::OffsetsListed { .. }
            | ExpectedEvent::RecordsDeleted { .. }
            | ExpectedEvent::RecordsBatchDeleted { .. }
            | ExpectedEvent::ClusterDescribed(_)
            | ExpectedEvent::FeaturesDescribed(_)
            | ExpectedEvent::FeatureUpdatesValidated(_)
            | ExpectedEvent::DelegationTokenLifecycleExercised(_)
            | ExpectedEvent::StreamsGroupAdminLifecycleExercised(_)
            | ExpectedEvent::MetadataQuorumDescribed(_)
            | ExpectedEvent::ProducerStatesDescribed { .. }
            | ExpectedEvent::LogDirsDescribed { .. }
            | ExpectedEvent::ReplicaLogDirsDescribed(..)
            | ExpectedEvent::ReplicaLogDirsAltered(_)
            | ExpectedEvent::TransactionsListed(_)
            | ExpectedEvent::TransactionsDescribed(..)
            | ExpectedEvent::ProducersFenced(..)
            | ExpectedEvent::PartitionReassignmentsAltered(_)
            | ExpectedEvent::PartitionReassignmentsListed(_)
            | ExpectedEvent::LeadersElected(_)
            | ExpectedEvent::ConsumerGroupsListed { .. }
            | ExpectedEvent::ConsumerGroupDescribed { .. }
            | ExpectedEvent::ConsumerGroupOffsetListed { .. }
            | ExpectedEvent::ConsumerGroupOffsetAltered { .. }
            | ExpectedEvent::ConsumerGroupOffsetDeleted { .. }
            | ExpectedEvent::ConsumerGroupDeleted { .. }
    )
}

fn event_is_admin(event: &AdapterEvent) -> bool {
    matches!(
        event,
        AdapterEvent::TopicCreated(_)
            | AdapterEvent::TopicCreationValidated(_)
            | AdapterEvent::TopicsCreationCompleted(_)
            | AdapterEvent::TopicPartitionsCreated(_)
            | AdapterEvent::TopicPartitionIncreaseValidated(_)
            | AdapterEvent::TopicDeleted(_)
            | AdapterEvent::TopicsDeleted(_)
            | AdapterEvent::TopicDescribed(_)
            | AdapterEvent::TopicsDescribed(_)
            | AdapterEvent::TopicsListed(_)
            | AdapterEvent::ConfigResourcesListed(_)
            | AdapterEvent::OffsetListed(_)
            | AdapterEvent::OffsetsListed(_)
            | AdapterEvent::RecordsDeleted(_)
            | AdapterEvent::RecordsBatchDeleted(_)
            | AdapterEvent::ClusterDescribed(_)
            | AdapterEvent::FeaturesDescribed(_)
            | AdapterEvent::FeatureUpdatesValidated(_)
            | AdapterEvent::DelegationTokenLifecycleExercised(_)
            | AdapterEvent::StreamsGroupAdminLifecycleExercised(_)
            | AdapterEvent::MetadataQuorumDescribed(_)
            | AdapterEvent::ProducersDescribed(_)
            | AdapterEvent::LogDirsDescribed(_)
            | AdapterEvent::ReplicaLogDirsDescribed(_)
            | AdapterEvent::ReplicaLogDirsAltered(_)
            | AdapterEvent::TransactionsListed(_)
            | AdapterEvent::TransactionsDescribed(_)
            | AdapterEvent::ProducersFenced(_)
            | AdapterEvent::PartitionReassignmentsAltered(_)
            | AdapterEvent::PartitionReassignmentsListed(_)
            | AdapterEvent::LeadersElected(_)
            | AdapterEvent::ConsumerGroupsListed(_)
            | AdapterEvent::ConsumerGroupDescribed(_)
            | AdapterEvent::ConsumerGroupOffsetListed(_)
            | AdapterEvent::ConsumerGroupOffsetAltered(_)
            | AdapterEvent::ConsumerGroupOffsetDeleted(_)
            | AdapterEvent::ConsumerGroupDeleted(_)
    )
}
