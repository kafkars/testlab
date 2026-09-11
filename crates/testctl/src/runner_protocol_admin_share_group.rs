//! Share-group Admin terminals require exact operation and resource identities.

use testlab_schema::AdapterEvent;

use crate::run_error::RunFailure;
use crate::runner_protocol::{EventDisposition, ExpectedEvent};
use crate::runner_protocol_identity::identity_result;

pub(super) fn classify(
    expected: &ExpectedEvent,
    event: &AdapterEvent,
) -> Option<Result<EventDisposition, RunFailure>> {
    let matches = match (expected, event) {
        (
            ExpectedEvent::ShareGroupDescribed {
                operation_id,
                group_id,
            },
            AdapterEvent::ShareGroupDescribed(actual),
        ) => operation_id == &actual.operation_id && group_id == &actual.group_id,
        (
            ExpectedEvent::ShareGroupOffsetsListed {
                operation_id,
                group_id,
                topic,
                partition,
            },
            AdapterEvent::ShareGroupOffsetsListed(actual),
        ) => {
            operation_id == &actual.operation_id
                && group_id == &actual.group_id
                && topic == &actual.topic
                && partition == &actual.partition
        }
        (
            ExpectedEvent::ShareGroupOffsetsAltered {
                operation_id,
                group_id,
                topic,
                partition,
            },
            AdapterEvent::ShareGroupOffsetsAltered(actual),
        ) => {
            operation_id == &actual.operation_id
                && group_id == &actual.group_id
                && topic == &actual.topic
                && partition == &actual.partition
        }
        (
            ExpectedEvent::ShareGroupOffsetsDeleted {
                operation_id,
                group_id,
                topic,
            },
            AdapterEvent::ShareGroupOffsetsDeleted(actual),
        ) => {
            operation_id == &actual.operation_id
                && group_id == &actual.group_id
                && topic == &actual.topic
        }
        _ => return None,
    };
    Some(identity_result(matches, event, expected))
}

pub(super) fn same_event_family(expected: &ExpectedEvent, event: &AdapterEvent) -> bool {
    matches!(
        (expected, event),
        (
            ExpectedEvent::ShareGroupDescribed { .. },
            AdapterEvent::ShareGroupDescribed(_)
        ) | (
            ExpectedEvent::ShareGroupOffsetsListed { .. },
            AdapterEvent::ShareGroupOffsetsListed(_)
        ) | (
            ExpectedEvent::ShareGroupOffsetsAltered { .. },
            AdapterEvent::ShareGroupOffsetsAltered(_)
        ) | (
            ExpectedEvent::ShareGroupOffsetsDeleted { .. },
            AdapterEvent::ShareGroupOffsetsDeleted(_)
        )
    )
}
