//! Protocol event outcomes separate intermediate public facts from command completion.

use testlab_schema::AdapterEvent;

use crate::run_error::RunFailure;
use crate::runner_protocol::ExpectedEvent;
use crate::runner_protocol_family::same_event_family;

/// Whether one correlated adapter event completes the active command.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum EventDisposition {
    Continue,
    Complete,
}

pub(super) fn classify_core(
    expected: &ExpectedEvent,
    event: &AdapterEvent,
) -> Result<EventDisposition, RunFailure> {
    match (expected, event) {
        (ExpectedEvent::Ready, AdapterEvent::Ready { .. })
        | (ExpectedEvent::Finished, AdapterEvent::Finished)
        | (ExpectedEvent::Aborted, AdapterEvent::Aborted) => Ok(EventDisposition::Complete),
        (ExpectedEvent::ClientCreated(expected), AdapterEvent::ClientCreated(observation))
            if expected == &observation.client_id =>
        {
            Ok(EventDisposition::Complete)
        }
        (ExpectedEvent::ClientReady(expected), AdapterEvent::ClientReady { client_id })
            if expected == client_id =>
        {
            Ok(EventDisposition::Complete)
        }
        (
            ExpectedEvent::ClientMetricsObserved(client_id, operation_id),
            AdapterEvent::ClientMetricsObserved(observation),
        ) if client_id == &observation.client_id && operation_id == &observation.operation_id => {
            Ok(EventDisposition::Complete)
        }
        (
            ExpectedEvent::ProducerCreated(expected),
            AdapterEvent::ProducerCreated { producer_id },
        ) if expected == producer_id => Ok(EventDisposition::Complete),
        (ExpectedEvent::FlushCompleted(expected), AdapterEvent::FlushCompleted { producer_id })
            if expected == producer_id =>
        {
            Ok(EventDisposition::Complete)
        }
        (ExpectedEvent::ProducerClosed(expected), AdapterEvent::ProducerClosed { producer_id })
            if expected == producer_id =>
        {
            Ok(EventDisposition::Complete)
        }
        (ExpectedEvent::ClientShutdown(expected), AdapterEvent::ClientShutdown { client_id })
            if expected == client_id =>
        {
            Ok(EventDisposition::Complete)
        }
        (
            ExpectedEvent::SendSettled(expected),
            AdapterEvent::OperationAccepted { operation_id },
        ) if expected == operation_id => Ok(EventDisposition::Continue),
        (
            ExpectedEvent::SendSettled(expected),
            AdapterEvent::OperationRejected { operation_id, .. }
            | AdapterEvent::OperationTerminal { operation_id, .. },
        ) if expected == operation_id => Ok(EventDisposition::Complete),
        (
            ExpectedEvent::BatchCompleted { operation_ids, .. },
            AdapterEvent::OperationAccepted { operation_id }
            | AdapterEvent::OperationRejected { operation_id, .. }
            | AdapterEvent::OperationTerminal { operation_id, .. },
        ) if operation_ids.contains(operation_id) => Ok(EventDisposition::Continue),
        (
            ExpectedEvent::BatchCompleted { producer_id, .. },
            AdapterEvent::BatchCompleted {
                producer_id: actual,
            },
        ) if producer_id == actual => Ok(EventDisposition::Complete),
        (
            ExpectedEvent::AssignedConsumerCreated(expected),
            AdapterEvent::AssignedConsumerCreated {
                consumer_id: actual,
            },
        ) if expected == actual => Ok(EventDisposition::Complete),
        (
            ExpectedEvent::AssignmentCompleted(expected),
            AdapterEvent::AssignmentCompleted {
                consumer_id: actual,
            },
        ) if expected == actual => Ok(EventDisposition::Complete),
        (
            ExpectedEvent::ReceiveCompleted(expected),
            AdapterEvent::ReceiveCompleted {
                receive_id: actual, ..
            },
        ) if expected == actual => Ok(EventDisposition::Complete),
        (
            ExpectedEvent::AssignedRecordTransferCompleted(expected),
            AdapterEvent::OperationAccepted { operation_id }
            | AdapterEvent::OperationTerminal { operation_id, .. },
        ) if expected == operation_id => Ok(EventDisposition::Continue),
        (
            ExpectedEvent::AssignedRecordTransferCompleted(expected),
            AdapterEvent::AssignedRecordTransferCompleted(completion),
        ) if expected == &completion.operation_id => Ok(EventDisposition::Complete),
        (
            ExpectedEvent::AssignedConsumerClosed(expected),
            AdapterEvent::AssignedConsumerClosed {
                consumer_id: actual,
            },
        ) if expected == actual => Ok(EventDisposition::Complete),
        _ if same_event_family(expected, event) => Err(RunFailure::protocol(
            "event_identity_mismatch",
            format!("event {event:?} does not match expected {expected:?}"),
        )),
        _ => Err(RunFailure::protocol(
            "unexpected_event_kind",
            format!("unexpected event {event:?} while waiting for {expected:?}"),
        )),
    }
}
