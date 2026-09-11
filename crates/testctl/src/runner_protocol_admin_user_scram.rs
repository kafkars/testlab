//! User SCRAM terminals require the exact operation identity and event family.

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
            ExpectedEvent::UserScramCredentialAltered(expected),
            AdapterEvent::UserScramCredentialAltered(actual),
        ) => expected == &actual.operation_id,
        (
            ExpectedEvent::UserScramCredentialDescribed(expected),
            AdapterEvent::UserScramCredentialDescribed(actual),
        ) => expected == &actual.operation_id,
        _ => return None,
    };
    Some(identity_result(matches, event, expected))
}

pub(super) fn same_event_family(expected: &ExpectedEvent, event: &AdapterEvent) -> bool {
    let expected = matches!(
        expected,
        ExpectedEvent::UserScramCredentialAltered(_)
            | ExpectedEvent::UserScramCredentialDescribed(_)
    );
    let actual = matches!(
        event,
        AdapterEvent::UserScramCredentialAltered(_) | AdapterEvent::UserScramCredentialDescribed(_)
    );
    expected && actual
}
