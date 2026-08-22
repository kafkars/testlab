//! Expected event shapes constrain each sequential protocol-v9 command.

use std::collections::BTreeSet;

use testlab_schema::{AdapterEvent, ClientId, ConsumerId, OperationId, ProducerId};

use crate::run_error::RunFailure;

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

fn classify_admin(
    expected: &ExpectedEvent,
    event: &AdapterEvent,
) -> Option<Result<EventDisposition, RunFailure>> {
    let (
        ExpectedEvent::TopicCreated {
            operation_id: expected_operation,
            topic: expected_topic,
        },
        AdapterEvent::TopicCreated {
            operation_id: actual_operation,
            topic: actual_topic,
        },
    ) = (expected, event)
    else {
        return None;
    };
    Some(
        if expected_operation == actual_operation && expected_topic == actual_topic {
            Ok(EventDisposition::Complete)
        } else {
            Err(RunFailure::protocol(
                "event_identity_mismatch",
                format!("event {event:?} does not match expected {expected:?}"),
            ))
        },
    )
}

fn classify_group(
    expected: &ExpectedEvent,
    event: &AdapterEvent,
) -> Option<Result<EventDisposition, RunFailure>> {
    let identity_matches = match (expected, event) {
        (
            ExpectedEvent::GroupConsumerCreated(expected),
            AdapterEvent::GroupConsumerCreated {
                consumer_id: actual,
            },
        )
        | (
            ExpectedEvent::GroupConsumerClosed(expected),
            AdapterEvent::GroupConsumerClosed {
                consumer_id: actual,
            },
        ) => expected == actual,
        (
            ExpectedEvent::GroupReceiveCompleted(expected),
            AdapterEvent::GroupReceiveCompleted {
                receive_id: actual, ..
            },
        ) => expected == actual,
        _ => return None,
    };
    Some(if identity_matches {
        Ok(EventDisposition::Complete)
    } else {
        Err(RunFailure::protocol(
            "event_identity_mismatch",
            format!("event {event:?} does not match expected {expected:?}"),
        ))
    })
}

fn same_event_family(expected: &ExpectedEvent, event: &AdapterEvent) -> bool {
    matches!(
        (expected, event),
        (ExpectedEvent::Ready, AdapterEvent::Ready { .. })
            | (
                ExpectedEvent::ClientReady(_),
                AdapterEvent::ClientReady { .. }
            )
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
                ExpectedEvent::BatchCompleted { .. },
                AdapterEvent::OperationAccepted { .. }
                    | AdapterEvent::OperationRejected { .. }
                    | AdapterEvent::OperationTerminal { .. }
                    | AdapterEvent::BatchCompleted { .. }
            )
            | (
                ExpectedEvent::AssignedConsumerCreated(_),
                AdapterEvent::AssignedConsumerCreated { .. }
            )
            | (
                ExpectedEvent::AssignmentCompleted(_),
                AdapterEvent::AssignmentCompleted { .. }
            )
            | (
                ExpectedEvent::ReceiveCompleted(_),
                AdapterEvent::ReceiveCompleted { .. }
            )
            | (
                ExpectedEvent::AssignedConsumerClosed(_),
                AdapterEvent::AssignedConsumerClosed { .. }
            )
            | (
                ExpectedEvent::GroupConsumerCreated(_),
                AdapterEvent::GroupConsumerCreated { .. }
            )
            | (
                ExpectedEvent::GroupReceiveCompleted(_),
                AdapterEvent::GroupReceiveCompleted { .. }
            )
            | (
                ExpectedEvent::GroupConsumerClosed(_),
                AdapterEvent::GroupConsumerClosed { .. }
            )
            | (
                ExpectedEvent::TopicCreated { .. },
                AdapterEvent::TopicCreated { .. }
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
