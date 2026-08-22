//! Expected event shapes constrain each sequential protocol-v11 command.

use std::collections::BTreeSet;

use testlab_schema::{AdapterEvent, ClientId, ConsumerId, OperationId, ProducerId};

use crate::run_error::RunFailure;
use crate::runner_protocol_family::{
    classify_admin, classify_group, classify_transaction, same_event_family,
};

#[derive(Clone, Debug)]
pub(crate) enum ExpectedEvent {
    Ready,
    ClientCreated(ClientId),
    ClientReady(ClientId),
    ProducerCreated(ProducerId),
    SendSettled(OperationId),
    BatchCompleted {
        producer_id: ProducerId,
        operation_ids: BTreeSet<OperationId>,
    },
    AssignedConsumerCreated(ConsumerId),
    AssignmentCompleted(ConsumerId),
    ReceiveCompleted(OperationId),
    AssignedConsumerClosed(ConsumerId),
    GroupConsumerCreated(ConsumerId),
    GroupReceiveCompleted(OperationId),
    GroupConsumerClosed(ConsumerId),
    TopicCreated {
        operation_id: OperationId,
        topic: String,
    },
    TransactionalProducerCreated(ProducerId),
    TransactionCompleted {
        transaction_id: OperationId,
        operation_ids: BTreeSet<OperationId>,
    },
    TransactionalProducerClosed(ProducerId),
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
        if matches!(event, AdapterEvent::CommandFailed { .. }) {
            return Ok(EventDisposition::Complete);
        }
        if let Some(disposition) = classify_group(self, event) {
            return disposition;
        }
        if let Some(disposition) = classify_admin(self, event) {
            return disposition;
        }
        if let Some(disposition) = classify_transaction(self, event) {
            return disposition;
        }
        match (self, event) {
            (Self::Ready, AdapterEvent::Ready { .. })
            | (Self::Finished, AdapterEvent::Finished) => Ok(EventDisposition::Complete),
            (Self::ClientCreated(expected), AdapterEvent::ClientCreated { client_id })
                if expected == client_id =>
            {
                Ok(EventDisposition::Complete)
            }
            (Self::ClientReady(expected), AdapterEvent::ClientReady { client_id })
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
            (
                Self::BatchCompleted { operation_ids, .. },
                AdapterEvent::OperationAccepted { operation_id }
                | AdapterEvent::OperationRejected { operation_id, .. }
                | AdapterEvent::OperationTerminal { operation_id, .. },
            ) if operation_ids.contains(operation_id) => Ok(EventDisposition::Continue),
            (
                Self::BatchCompleted { producer_id, .. },
                AdapterEvent::BatchCompleted {
                    producer_id: actual,
                },
            ) if producer_id == actual => Ok(EventDisposition::Complete),
            (
                Self::AssignedConsumerCreated(expected),
                AdapterEvent::AssignedConsumerCreated {
                    consumer_id: actual,
                },
            ) if expected == actual => Ok(EventDisposition::Complete),
            (
                Self::AssignmentCompleted(expected),
                AdapterEvent::AssignmentCompleted {
                    consumer_id: actual,
                },
            ) if expected == actual => Ok(EventDisposition::Complete),
            (
                Self::ReceiveCompleted(expected),
                AdapterEvent::ReceiveCompleted {
                    receive_id: actual, ..
                },
            ) if expected == actual => Ok(EventDisposition::Complete),
            (
                Self::AssignedConsumerClosed(expected),
                AdapterEvent::AssignedConsumerClosed {
                    consumer_id: actual,
                },
            ) if expected == actual => Ok(EventDisposition::Complete),
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
