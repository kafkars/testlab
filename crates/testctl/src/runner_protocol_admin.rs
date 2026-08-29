//! Admin protocol matching validates stable identities, leaving result truth to verification.

use testlab_schema::AdapterEvent;

use crate::run_error::RunFailure;
use crate::runner_protocol::{EventDisposition, ExpectedEvent};

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
            ExpectedEvent::ClusterDescribed { operation_id },
            AdapterEvent::ClusterDescribed(actual),
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
            | ExpectedEvent::TopicDescribed { .. }
            | ExpectedEvent::TopicsListed { .. }
            | ExpectedEvent::OffsetListed { .. }
            | ExpectedEvent::RecordsDeleted { .. }
            | ExpectedEvent::ClusterDescribed { .. }
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
            | AdapterEvent::TopicDescribed(_)
            | AdapterEvent::TopicsListed(_)
            | AdapterEvent::OffsetListed(_)
            | AdapterEvent::RecordsDeleted(_)
            | AdapterEvent::ClusterDescribed(_)
            | AdapterEvent::ConsumerGroupsListed(_)
            | AdapterEvent::ConsumerGroupDescribed(_)
            | AdapterEvent::ConsumerGroupOffsetListed(_)
            | AdapterEvent::ConsumerGroupOffsetAltered(_)
            | AdapterEvent::ConsumerGroupOffsetDeleted(_)
            | AdapterEvent::ConsumerGroupDeleted(_)
    )
}
