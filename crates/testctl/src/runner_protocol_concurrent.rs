//! Concurrent protocol classification preserves exact actor and operation membership.

use std::collections::BTreeSet;

use testlab_schema::{ActorId, AdapterEvent, ConcurrencyId, OperationId};

use crate::run_error::RunFailure;
use crate::runner_protocol::{EventDisposition, ExpectedEvent};

#[derive(Clone, Debug)]
pub(crate) struct ConcurrentExpectation {
    pub(crate) concurrency_id: ConcurrencyId,
    pub(crate) actors: Vec<(ActorId, OperationId)>,
    pub(crate) sends: BTreeSet<OperationId>,
    pub(crate) receives: BTreeSet<OperationId>,
}

impl ConcurrentExpectation {
    pub(crate) fn actor_ids(&self) -> Vec<ActorId> {
        self.actors.iter().map(|(actor, _)| actor.clone()).collect()
    }
}

pub(crate) fn classify(
    expected: &ExpectedEvent,
    event: &AdapterEvent,
) -> Option<Result<EventDisposition, RunFailure>> {
    let (ExpectedEvent::ConcurrentActorsStarted(expectation)
    | ExpectedEvent::ConcurrentActorsCompleted(expectation)) = expected
    else {
        return None;
    };
    let valid = match event {
        AdapterEvent::ConcurrentActorsStarted {
            concurrency_id,
            actor_ids,
        } if matches!(expected, ExpectedEvent::ConcurrentActorsStarted(_)) => {
            concurrency_id == &expectation.concurrency_id && actor_ids == &expectation.actor_ids()
        }
        AdapterEvent::OperationAccepted { operation_id }
        | AdapterEvent::OperationRejected { operation_id, .. }
        | AdapterEvent::OperationTerminal { operation_id, .. }
            if matches!(expected, ExpectedEvent::ConcurrentActorsCompleted(_)) =>
        {
            expectation.sends.contains(operation_id)
        }
        AdapterEvent::ReceiveCompleted { receive_id, .. }
            if matches!(expected, ExpectedEvent::ConcurrentActorsCompleted(_)) =>
        {
            expectation.receives.contains(receive_id)
        }
        AdapterEvent::ConcurrentActorCompleted {
            concurrency_id,
            actor_id,
            operation_id,
        } if matches!(expected, ExpectedEvent::ConcurrentActorsCompleted(_)) => {
            concurrency_id == &expectation.concurrency_id
                && expectation
                    .actors
                    .iter()
                    .any(|(actor, operation)| actor == actor_id && operation == operation_id)
        }
        AdapterEvent::ConcurrentActorsCompleted {
            concurrency_id,
            actor_ids,
        } if matches!(expected, ExpectedEvent::ConcurrentActorsCompleted(_)) => {
            concurrency_id == &expectation.concurrency_id && actor_ids == &expectation.actor_ids()
        }
        _ => return None,
    };
    if !valid {
        return Some(Err(identity_mismatch(event, expected)));
    }
    Some(Ok(
        if matches!(event, AdapterEvent::ConcurrentActorsStarted { .. })
            || matches!(event, AdapterEvent::ConcurrentActorsCompleted { .. })
        {
            EventDisposition::Complete
        } else {
            EventDisposition::Continue
        },
    ))
}

pub(crate) fn same_event_family(expected: &ExpectedEvent, event: &AdapterEvent) -> bool {
    match expected {
        ExpectedEvent::ConcurrentActorsStarted(_) => {
            matches!(event, AdapterEvent::ConcurrentActorsStarted { .. })
        }
        ExpectedEvent::ConcurrentActorsCompleted(_) => matches!(
            event,
            AdapterEvent::OperationAccepted { .. }
                | AdapterEvent::OperationRejected { .. }
                | AdapterEvent::OperationTerminal { .. }
                | AdapterEvent::ReceiveCompleted { .. }
                | AdapterEvent::ConcurrentActorCompleted { .. }
                | AdapterEvent::ConcurrentActorsCompleted { .. }
        ),
        _ => false,
    }
}

fn identity_mismatch(event: &AdapterEvent, expected: &ExpectedEvent) -> RunFailure {
    RunFailure::protocol(
        "event_identity_mismatch",
        format!("event {event:?} does not match expected {expected:?}"),
    )
}
