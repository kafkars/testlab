//! ACL protocol classification correlates each terminal by exact operation identity.

use testlab_schema::AdapterEvent;

use crate::run_error::RunFailure;
use crate::runner_protocol::{EventDisposition, ExpectedEvent};
use crate::runner_protocol_identity::identity_result;

pub(super) fn classify(
    expected: &ExpectedEvent,
    event: &AdapterEvent,
) -> Option<Result<EventDisposition, RunFailure>> {
    let matches = match (expected, event) {
        (ExpectedEvent::AclsCreated(expected), AdapterEvent::AclsCreated(actual)) => {
            expected == &actual.operation_id
        }
        (ExpectedEvent::AclsDescribed(expected), AdapterEvent::AclsDescribed(actual)) => {
            expected == &actual.operation_id
        }
        (ExpectedEvent::AclsDeleted(expected), AdapterEvent::AclsDeleted(actual)) => {
            expected == &actual.operation_id
        }
        _ => return None,
    };
    Some(identity_result(matches, event, expected))
}

pub(super) fn same_event_family(expected: &ExpectedEvent, event: &AdapterEvent) -> bool {
    let expected = matches!(
        expected,
        ExpectedEvent::AclsCreated(_)
            | ExpectedEvent::AclsDescribed(_)
            | ExpectedEvent::AclsDeleted(_)
    );
    let actual = matches!(
        event,
        AdapterEvent::AclsCreated(_)
            | AdapterEvent::AclsDescribed(_)
            | AdapterEvent::AclsDeleted(_)
    );
    expected && actual
}
