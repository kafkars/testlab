//! Client-quota terminals require the exact operation identity and event family.

use testlab_schema::AdapterEvent;

use crate::run_error::RunFailure;
use crate::runner_protocol::{EventDisposition, ExpectedEvent};
use crate::runner_protocol_identity::identity_result;

pub(super) fn classify(
    expected: &ExpectedEvent,
    event: &AdapterEvent,
) -> Option<Result<EventDisposition, RunFailure>> {
    let matches = match (expected, event) {
        (ExpectedEvent::ClientQuotaAltered(expected), AdapterEvent::ClientQuotaAltered(actual)) => {
            expected == &actual.operation_id
        }
        (
            ExpectedEvent::ClientQuotaDescribed(expected),
            AdapterEvent::ClientQuotaDescribed(actual),
        ) => expected == &actual.operation_id,
        _ => return None,
    };
    Some(identity_result(matches, event, expected))
}

pub(super) fn same_event_family(expected: &ExpectedEvent, event: &AdapterEvent) -> bool {
    let expected = matches!(
        expected,
        ExpectedEvent::ClientQuotaAltered(_) | ExpectedEvent::ClientQuotaDescribed(_)
    );
    let actual = matches!(
        event,
        AdapterEvent::ClientQuotaAltered(_) | AdapterEvent::ClientQuotaDescribed(_)
    );
    expected && actual
}
