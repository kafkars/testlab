//! Admin protocol matching validates stable identities, leaving result truth to verification.

use testlab_schema::AdapterEvent;

use crate::run_error::RunFailure;
use crate::runner_protocol::{EventDisposition, ExpectedEvent};

pub(super) fn classify_admin(
    expected: &ExpectedEvent,
    event: &AdapterEvent,
) -> Option<Result<EventDisposition, RunFailure>> {
    let identity_matches = match (expected, event) {
        (
            ExpectedEvent::TopicCreated {
                operation_id: expected_operation,
                topic: expected_topic,
            },
            AdapterEvent::TopicCreated {
                operation_id: actual_operation,
                topic: actual_topic,
            },
        )
        | (
            ExpectedEvent::TopicPartitionsCreated {
                operation_id: expected_operation,
                topic: expected_topic,
            },
            AdapterEvent::TopicPartitionsCreated {
                operation_id: actual_operation,
                topic: actual_topic,
            },
        )
        | (
            ExpectedEvent::TopicDescribed {
                operation_id: expected_operation,
                topic: expected_topic,
            },
            AdapterEvent::TopicDescribed {
                operation_id: actual_operation,
                topic: actual_topic,
                ..
            },
        ) => expected_operation == actual_operation && expected_topic == actual_topic,
        (
            ExpectedEvent::TopicsListed {
                operation_id: expected_operation,
            },
            AdapterEvent::TopicsListed {
                operation_id: actual_operation,
                ..
            },
        ) => expected_operation == actual_operation,
        (
            ExpectedEvent::OffsetListed {
                operation_id: expected_operation,
                topic: expected_topic,
                partition: expected_partition,
            },
            AdapterEvent::OffsetListed {
                operation_id: actual_operation,
                topic: actual_topic,
                partition: actual_partition,
                ..
            },
        ) => {
            expected_operation == actual_operation
                && expected_topic == actual_topic
                && expected_partition == actual_partition
        }
        (
            ExpectedEvent::ConsumerGroupOffsetListed {
                operation_id: expected_operation,
                group_id: expected_group,
                topic: expected_topic,
                partition: expected_partition,
            },
            AdapterEvent::ConsumerGroupOffsetListed {
                operation_id: actual_operation,
                group_id: actual_group,
                topic: actual_topic,
                partition: actual_partition,
                ..
            },
        ) => {
            expected_operation == actual_operation
                && expected_group == actual_group
                && expected_topic == actual_topic
                && expected_partition == actual_partition
        }
        _ => return None,
    };
    Some(if identity_matches {
        Ok(EventDisposition::Complete)
    } else {
        Err(RunFailure::protocol(
            "event_identity_mismatch",
            format!("event {event:?} does not match expected {expected:?}"),
        ))
    })
}

pub(super) fn same_admin_event_family(expected: &ExpectedEvent, event: &AdapterEvent) -> bool {
    matches!(
        expected,
        ExpectedEvent::TopicCreated { .. }
            | ExpectedEvent::TopicPartitionsCreated { .. }
            | ExpectedEvent::TopicDescribed { .. }
            | ExpectedEvent::TopicsListed { .. }
            | ExpectedEvent::OffsetListed { .. }
            | ExpectedEvent::ConsumerGroupOffsetListed { .. }
    ) && matches!(
        event,
        AdapterEvent::TopicCreated { .. }
            | AdapterEvent::TopicPartitionsCreated { .. }
            | AdapterEvent::TopicDescribed { .. }
            | AdapterEvent::TopicsListed { .. }
            | AdapterEvent::OffsetListed { .. }
            | AdapterEvent::ConsumerGroupOffsetListed { .. }
    )
}
