//! Share event classification fences every retained batch and lifecycle identity.

use testlab_schema::AdapterEvent;

use crate::run_error::RunFailure;
use crate::runner_protocol::{EventDisposition, ExpectedEvent};

pub(crate) fn classify(
    expected: &ExpectedEvent,
    event: &AdapterEvent,
) -> Option<Result<EventDisposition, RunFailure>> {
    let matches = match (expected, event) {
        (
            ExpectedEvent::ShareConsumerCreated(expected),
            AdapterEvent::ShareConsumerCreated {
                consumer_id: actual,
            },
        )
        | (
            ExpectedEvent::ShareConsumerClosed(expected),
            AdapterEvent::ShareConsumerClosed {
                consumer_id: actual,
                ..
            },
        ) => expected == actual,
        (
            ExpectedEvent::ShareReceiveCompleted(expected),
            AdapterEvent::ShareReceiveCompleted {
                receive_id: actual, ..
            },
        )
        | (
            ExpectedEvent::ShareAcknowledgementCompleted(expected),
            AdapterEvent::ShareAcknowledgementCompleted {
                acknowledgement_id: actual,
                ..
            },
        )
        | (
            ExpectedEvent::ShareBatchDropped(expected),
            AdapterEvent::ShareBatchDropped { receive_id: actual },
        ) => expected == actual,
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
