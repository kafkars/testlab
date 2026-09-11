//! Share-group description terminals require exact operation and group identities.

use testlab_schema::AdapterEvent;

use crate::run_error::RunFailure;
use crate::runner_protocol::{EventDisposition, ExpectedEvent};
use crate::runner_protocol_identity::identity_result;

pub(super) fn classify(
    expected: &ExpectedEvent,
    event: &AdapterEvent,
) -> Option<Result<EventDisposition, RunFailure>> {
    let (
        ExpectedEvent::ShareGroupDescribed {
            operation_id,
            group_id,
        },
        AdapterEvent::ShareGroupDescribed(actual),
    ) = (expected, event)
    else {
        return None;
    };
    Some(identity_result(
        operation_id == &actual.operation_id && group_id == &actual.group_id,
        event,
        expected,
    ))
}

pub(super) fn same_event_family(expected: &ExpectedEvent, event: &AdapterEvent) -> bool {
    matches!(expected, ExpectedEvent::ShareGroupDescribed { .. })
        && matches!(event, AdapterEvent::ShareGroupDescribed(_))
}
