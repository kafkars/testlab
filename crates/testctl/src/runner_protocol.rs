//! Expected event shapes constrain each sequential protocol-v1 command.

use testlab_schema::{AdapterEvent, ClientId, OperationId, ProducerId};

use crate::run_error::RunFailure;

#[derive(Clone, Debug)]
pub(crate) enum ExpectedEvent {
    Ready,
    ClientCreated(ClientId),
    ProducerCreated(ProducerId),
    SendSettled(OperationId),
    FlushCompleted(ProducerId),
    ProducerClosed(ProducerId),
    ClientShutdown(ClientId),
    Finished,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum EventDisposition {
    Continue,
    Complete,
}

impl ExpectedEvent {
    pub(crate) fn classify(&self, event: &AdapterEvent) -> Result<EventDisposition, RunFailure> {
        match (self, event) {
            (Self::Ready, AdapterEvent::Ready { .. })
            | (Self::Finished, AdapterEvent::Finished) => Ok(EventDisposition::Complete),
            (Self::ClientCreated(expected), AdapterEvent::ClientCreated { client_id })
                if expected == client_id =>
            {
                Ok(EventDisposition::Complete)
            }
            (Self::ProducerCreated(expected), AdapterEvent::ProducerCreated { producer_id })
                if expected == producer_id =>
            {
                Ok(EventDisposition::Complete)
            }
            (Self::FlushCompleted(expected), AdapterEvent::FlushCompleted { producer_id })
                if expected == producer_id =>
            {
                Ok(EventDisposition::Complete)
            }
            (Self::ProducerClosed(expected), AdapterEvent::ProducerClosed { producer_id })
                if expected == producer_id =>
            {
                Ok(EventDisposition::Complete)
            }
            (Self::ClientShutdown(expected), AdapterEvent::ClientShutdown { client_id })
                if expected == client_id =>
            {
                Ok(EventDisposition::Complete)
            }
            (Self::SendSettled(expected), AdapterEvent::OperationAccepted { operation_id })
                if expected == operation_id =>
            {
                Ok(EventDisposition::Continue)
            }
            (
                Self::SendSettled(expected),
                AdapterEvent::OperationRejected { operation_id, .. }
                | AdapterEvent::OperationTerminal { operation_id, .. },
            ) if expected == operation_id => Ok(EventDisposition::Complete),
            _ if same_event_family(self, event) => Err(RunFailure::protocol(
                "event_identity_mismatch",
                format!("event {event:?} does not match expected {self:?}"),
            )),
            _ => Err(RunFailure::protocol(
                "unexpected_event_kind",
                format!("unexpected event {event:?} while waiting for {self:?}"),
            )),
        }
    }
}

fn same_event_family(expected: &ExpectedEvent, event: &AdapterEvent) -> bool {
    matches!(
        (expected, event),
        (ExpectedEvent::Ready, AdapterEvent::Ready { .. })
            | (
                ExpectedEvent::ClientCreated(_),
                AdapterEvent::ClientCreated { .. }
            )
            | (
                ExpectedEvent::ProducerCreated(_),
                AdapterEvent::ProducerCreated { .. }
            )
            | (
                ExpectedEvent::SendSettled(_),
                AdapterEvent::OperationAccepted { .. }
                    | AdapterEvent::OperationRejected { .. }
                    | AdapterEvent::OperationTerminal { .. }
            )
            | (
                ExpectedEvent::FlushCompleted(_),
                AdapterEvent::FlushCompleted { .. }
            )
            | (
                ExpectedEvent::ProducerClosed(_),
                AdapterEvent::ProducerClosed { .. }
            )
            | (
                ExpectedEvent::ClientShutdown(_),
                AdapterEvent::ClientShutdown { .. }
            )
            | (ExpectedEvent::Finished, AdapterEvent::Finished)
    )
}
