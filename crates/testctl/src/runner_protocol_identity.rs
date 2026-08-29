//! Protocol identity failures retain one stable classification and diagnostic shape.

use testlab_schema::AdapterEvent;

use crate::run_error::RunFailure;
use crate::runner_protocol::{EventDisposition, ExpectedEvent};

pub(super) fn identity_result(
    matches: bool,
    event: &AdapterEvent,
    expected: &ExpectedEvent,
) -> Result<EventDisposition, RunFailure> {
    if matches {
        Ok(EventDisposition::Complete)
    } else {
        Err(identity_mismatch(event, expected))
    }
}

pub(super) fn identity_mismatch(event: &AdapterEvent, expected: &ExpectedEvent) -> RunFailure {
    RunFailure::protocol(
        "event_identity_mismatch",
        format!("event {event:?} does not match expected {expected:?}"),
    )
}
