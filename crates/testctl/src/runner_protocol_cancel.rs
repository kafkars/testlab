//! Producer cancellation classification preserves its correlated event sequence.

use testlab_schema::AdapterEvent;

use crate::run_error::RunFailure;
use crate::runner_protocol::{EventDisposition, ExpectedEvent};

pub(crate) fn classify(
    expected: &ExpectedEvent,
    event: &AdapterEvent,
) -> Option<Result<EventDisposition, RunFailure>> {
    let ExpectedEvent::ProducerCancellationCompleted(expected_id) = expected else {
        return None;
    };
    let (actual_id, complete) = match event {
        AdapterEvent::OperationAccepted { operation_id }
        | AdapterEvent::OperationRejected { operation_id, .. }
        | AdapterEvent::OperationTerminal { operation_id, .. } => (operation_id, false),
        AdapterEvent::ProducerCancellationCompleted(completion) => (&completion.operation_id, true),
        _ => return None,
    };
    if expected_id != actual_id {
        return Some(Err(RunFailure::protocol(
            "event_identity_mismatch",
            format!("event {event:?} does not match expected {expected:?}"),
        )));
    }
    Some(Ok(if complete {
        EventDisposition::Complete
    } else {
        EventDisposition::Continue
    }))
}

pub(crate) fn same_event_family(expected: &ExpectedEvent, event: &AdapterEvent) -> bool {
    matches!(
        (expected, event),
        (
            ExpectedEvent::ProducerCancellationCompleted(_),
            AdapterEvent::OperationAccepted { .. }
                | AdapterEvent::OperationRejected { .. }
                | AdapterEvent::OperationTerminal { .. }
                | AdapterEvent::ProducerCancellationCompleted(_)
        )
    )
}
