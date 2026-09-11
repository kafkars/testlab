//! Active-producer completions retain every stable target identity.

use testlab_schema::{AdapterEvent, AdminProducersDescription};

use crate::run_error::RunFailure;
use crate::runner_protocol::{EventDisposition, ExpectedEvent};

pub(super) fn classify(
    expected: &ExpectedEvent,
    event: &AdapterEvent,
) -> Option<Result<EventDisposition, RunFailure>> {
    let (
        ExpectedEvent::ProducerStatesDescribed {
            operation_id,
            topic,
            partition,
        },
        AdapterEvent::ProducersDescribed(AdminProducersDescription {
            operation_id: actual_operation,
            topic: actual_topic,
            partition: actual_partition,
            ..
        }),
    ) = (expected, event)
    else {
        return None;
    };
    if operation_id == actual_operation && topic == actual_topic && partition == actual_partition {
        Some(Ok(EventDisposition::Complete))
    } else {
        Some(Err(RunFailure::protocol(
            "event_identity_mismatch",
            format!("event {event:?} does not match expected {expected:?}"),
        )))
    }
}
