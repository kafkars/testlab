//! Batched group-admin completion matching retains exact operation and resource identities.

use testlab_schema::AdapterEvent;

use crate::run_error::RunFailure;
use crate::runner_protocol::{EventDisposition, ExpectedEvent};

pub(super) fn classify(
    expected: &ExpectedEvent,
    event: &AdapterEvent,
) -> Option<Result<EventDisposition, RunFailure>> {
    let matches = match (expected, event) {
        (
            ExpectedEvent::ConsumerGroupOffsetsListed { operation_id },
            AdapterEvent::ConsumerGroupOffsetsListed(actual),
        ) => operation_id == &actual.operation_id,
        (
            ExpectedEvent::ConsumerGroupsOffsetsListed { operation_id },
            AdapterEvent::ConsumerGroupsOffsetsListed(actual),
        ) => operation_id == &actual.operation_id,
        (
            ExpectedEvent::ConsumerGroupOffsetsAltered { operation_id },
            AdapterEvent::ConsumerGroupOffsetsAltered(actual),
        )
        | (
            ExpectedEvent::ConsumerGroupOffsetsDeleted { operation_id },
            AdapterEvent::ConsumerGroupOffsetsDeleted(actual),
        ) => operation_id == &actual.operation_id,
        (
            ExpectedEvent::ClassicGroupsDescribed { operation_id },
            AdapterEvent::ClassicGroupsDescribed(actual),
        ) => operation_id == &actual.operation_id,
        (
            ExpectedEvent::ConsumerGroupsDescribed(operation_id),
            AdapterEvent::ConsumerGroupsDescribed(actual),
        ) => operation_id == &actual.operation_id,
        (
            ExpectedEvent::ConsumerGroupsDeleted {
                operation_id,
                group_ids,
            },
            AdapterEvent::ConsumerGroupsDeleted(actual),
        ) => {
            operation_id == &actual.operation_id
                && actual
                    .outcomes
                    .iter()
                    .map(|outcome| outcome.group_id.as_str())
                    .eq(group_ids.iter().map(String::as_str))
        }
        (
            ExpectedEvent::ConsumerGroupMembersRemoved(operation_id, group_instance_ids),
            AdapterEvent::ConsumerGroupMembersRemoved(actual),
        ) => {
            operation_id == &actual.operation_id
                && actual
                    .outcomes
                    .iter()
                    .map(|outcome| outcome.group_instance_id.as_str())
                    .eq(group_instance_ids.iter().map(String::as_str))
        }
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

pub(super) fn same_event_family(expected: &ExpectedEvent, event: &AdapterEvent) -> bool {
    expected_is_batch_group(expected) && event_is_batch_group(event)
}

fn expected_is_batch_group(expected: &ExpectedEvent) -> bool {
    matches!(
        expected,
        ExpectedEvent::ConsumerGroupOffsetsListed { .. }
            | ExpectedEvent::ConsumerGroupsOffsetsListed { .. }
            | ExpectedEvent::ConsumerGroupOffsetsAltered { .. }
            | ExpectedEvent::ConsumerGroupOffsetsDeleted { .. }
            | ExpectedEvent::ClassicGroupsDescribed { .. }
            | ExpectedEvent::ConsumerGroupsDescribed(_)
            | ExpectedEvent::ConsumerGroupsDeleted { .. }
            | ExpectedEvent::ConsumerGroupMembersRemoved(..)
    )
}

fn event_is_batch_group(event: &AdapterEvent) -> bool {
    matches!(
        event,
        AdapterEvent::ConsumerGroupOffsetsListed(_)
            | AdapterEvent::ConsumerGroupsOffsetsListed(_)
            | AdapterEvent::ConsumerGroupOffsetsAltered(_)
            | AdapterEvent::ConsumerGroupOffsetsDeleted(_)
            | AdapterEvent::ClassicGroupsDescribed(_)
            | AdapterEvent::ConsumerGroupsDescribed(_)
            | AdapterEvent::ConsumerGroupsDeleted(_)
            | AdapterEvent::ConsumerGroupMembersRemoved(_)
    )
}
