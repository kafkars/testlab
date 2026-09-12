//! Admin protocol matching validates stable identities, leaving result truth to verification.

use testlab_schema::AdapterEvent;

use crate::run_error::RunFailure;
use crate::runner_protocol::{EventDisposition, ExpectedEvent};
pub(super) use crate::runner_protocol_admin_family::same_admin_event_family;

#[allow(
    clippy::too_many_lines,
    reason = "the exhaustive classifier keeps every expected and actual admin event pair adjacent"
)]
pub(super) fn classify_admin(
    expected: &ExpectedEvent,
    event: &AdapterEvent,
) -> Option<Result<EventDisposition, RunFailure>> {
    let matches = match (expected, event) {
        (
            ExpectedEvent::TopicCreated {
                operation_id,
                topic,
            },
            AdapterEvent::TopicCreated(actual),
        )
        | (
            ExpectedEvent::TopicCreationValidated {
                operation_id,
                topic,
            },
            AdapterEvent::TopicCreationValidated(actual),
        )
        | (
            ExpectedEvent::TopicPartitionsCreated {
                operation_id,
                topic,
            },
            AdapterEvent::TopicPartitionsCreated(actual),
        )
        | (
            ExpectedEvent::TopicPartitionIncreaseValidated {
                operation_id,
                topic,
            },
            AdapterEvent::TopicPartitionIncreaseValidated(actual),
        )
        | (
            ExpectedEvent::TopicDeleted {
                operation_id,
                topic,
            },
            AdapterEvent::TopicDeleted(actual),
        ) => operation_id == &actual.operation_id && topic == &actual.topic,
        (
            ExpectedEvent::TopicsDeleted {
                operation_id,
                topics,
            },
            AdapterEvent::TopicsDeleted(actual),
        ) => {
            operation_id == &actual.operation_id
                && actual
                    .outcomes
                    .iter()
                    .map(|outcome| outcome.topic.as_str())
                    .eq(topics.iter().map(String::as_str))
        }
        (
            ExpectedEvent::TopicsCreationCompleted { operation_id },
            AdapterEvent::TopicsCreationCompleted(actual),
        ) => operation_id == &actual.operation_id,
        (
            ExpectedEvent::TopicDescribed {
                operation_id,
                topic,
            },
            AdapterEvent::TopicDescribed(actual),
        ) => operation_id == &actual.operation_id && topic == &actual.topic,
        (
            ExpectedEvent::TopicsDescribed {
                operation_id,
                topics,
            },
            AdapterEvent::TopicsDescribed(actual),
        ) => {
            operation_id == &actual.operation_id
                && actual
                    .outcomes
                    .iter()
                    .map(|outcome| outcome.topic.as_str())
                    .eq(topics.iter().map(String::as_str))
        }
        (ExpectedEvent::TopicsListed { operation_id }, AdapterEvent::TopicsListed(actual)) => {
            operation_id == &actual.operation_id
        }
        (
            ExpectedEvent::OffsetListed {
                operation_id,
                topic,
                partition,
            },
            AdapterEvent::OffsetListed(actual),
        ) => {
            operation_id == &actual.operation_id
                && topic == &actual.topic
                && partition == &actual.partition
        }
        (ExpectedEvent::OffsetsListed { operation_id }, AdapterEvent::OffsetsListed(actual)) => {
            operation_id == &actual.operation_id
        }
        (
            ExpectedEvent::RecordsDeleted {
                operation_id,
                topic,
                partition,
            },
            AdapterEvent::RecordsDeleted(actual),
        ) => {
            operation_id == &actual.operation_id
                && topic == &actual.topic
                && partition == &actual.partition
        }
        (
            ExpectedEvent::RecordsBatchDeleted {
                operation_id,
                targets,
            },
            AdapterEvent::RecordsBatchDeleted(actual),
        ) => {
            operation_id == &actual.operation_id
                && actual
                    .outcomes
                    .iter()
                    .map(|outcome| (outcome.topic.as_str(), outcome.partition))
                    .eq(targets
                        .iter()
                        .map(|(topic, partition)| (topic.as_str(), *partition)))
        }
        (ExpectedEvent::ClusterDescribed(operation_id), AdapterEvent::ClusterDescribed(actual)) => {
            operation_id == &actual.operation_id
        }
        (
            ExpectedEvent::PartitionReassignmentsAltered(operation_id),
            AdapterEvent::PartitionReassignmentsAltered(actual),
        ) => operation_id == &actual.operation_id,
        (
            ExpectedEvent::PartitionReassignmentsListed(operation_id),
            AdapterEvent::PartitionReassignmentsListed(actual),
        ) => operation_id == &actual.operation_id,
        (ExpectedEvent::LeadersElected(operation_id), AdapterEvent::LeadersElected(actual)) => {
            operation_id == &actual.operation_id
        }
        (
            ExpectedEvent::FeaturesDescribed(operation_id),
            AdapterEvent::FeaturesDescribed(actual),
        ) => operation_id == &actual.operation_id,
        (
            ExpectedEvent::FeatureUpdatesValidated(operation_id),
            AdapterEvent::FeatureUpdatesValidated(actual),
        ) => operation_id == &actual.operation_id,
        (
            ExpectedEvent::DelegationTokenLifecycleExercised(operation_id),
            AdapterEvent::DelegationTokenLifecycleExercised(actual),
        ) => operation_id == &actual.operation_id,
        (
            ExpectedEvent::StreamsGroupAdminLifecycleExercised(operation_id),
            AdapterEvent::StreamsGroupAdminLifecycleExercised(actual),
        ) => operation_id == &actual.operation_id,
        (
            ExpectedEvent::MetadataQuorumDescribed(operation_id),
            AdapterEvent::MetadataQuorumDescribed(actual),
        ) => operation_id == &actual.operation_id,
        (
            ExpectedEvent::ConsumerGroupsListed { operation_id },
            AdapterEvent::ConsumerGroupsListed(actual),
        ) => operation_id == &actual.operation_id,
        (
            ExpectedEvent::ConsumerGroupDescribed {
                operation_id,
                group_id,
            },
            AdapterEvent::ConsumerGroupDescribed(actual),
        ) => operation_id == &actual.operation_id && group_id == &actual.group_id,
        (
            ExpectedEvent::ConsumerGroupOffsetListed {
                operation_id,
                group_id,
                topic,
                partition,
            },
            AdapterEvent::ConsumerGroupOffsetListed(actual),
        ) => group_offset_matches(
            operation_id,
            group_id,
            topic,
            *partition,
            &actual.operation_id,
            &actual.group_id,
            &actual.topic,
            actual.partition,
        ),
        (
            ExpectedEvent::ConsumerGroupOffsetAltered {
                operation_id,
                group_id,
                topic,
                partition,
            },
            AdapterEvent::ConsumerGroupOffsetAltered(actual),
        )
        | (
            ExpectedEvent::ConsumerGroupOffsetDeleted {
                operation_id,
                group_id,
                topic,
                partition,
            },
            AdapterEvent::ConsumerGroupOffsetDeleted(actual),
        ) => group_offset_matches(
            operation_id,
            group_id,
            topic,
            *partition,
            &actual.operation_id,
            &actual.group_id,
            &actual.topic,
            actual.partition,
        ),
        (
            ExpectedEvent::ConsumerGroupDeleted {
                operation_id,
                group_id,
            },
            AdapterEvent::ConsumerGroupDeleted(actual),
        ) => operation_id == &actual.operation_id && group_id == &actual.group_id,
        _ => return None,
    };
    Some(if matches {
        Ok(EventDisposition::Complete)
    } else {
        Err(RunFailure::protocol(
            "event_identity_mismatch",
            format!("event {event:?} does not match expected {expected:?}"),
        ))
    })
}

#[allow(
    clippy::too_many_arguments,
    reason = "protocol correlation compares every expected and actual group-offset identity"
)]
fn group_offset_matches(
    expected_operation: &testlab_schema::OperationId,
    expected_group: &str,
    expected_topic: &str,
    expected_partition: i32,
    actual_operation: &testlab_schema::OperationId,
    actual_group: &str,
    actual_topic: &str,
    actual_partition: i32,
) -> bool {
    expected_operation == actual_operation
        && expected_group == actual_group
        && expected_topic == actual_topic
        && expected_partition == actual_partition
}
