//! Event-family classification separates identity checks from session flow.

use testlab_schema::AdapterEvent;

use crate::run_error::RunFailure;
use crate::runner_protocol::{EventDisposition, ExpectedEvent};
use crate::runner_protocol_admin::same_admin_event_family;

pub(super) fn classify_group(
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
        Err(identity_mismatch(event, expected))
    })
}

pub(super) fn classify_transaction(
    expected: &ExpectedEvent,
    event: &AdapterEvent,
) -> Option<Result<EventDisposition, RunFailure>> {
    let matches = match (expected, event) {
        (
            ExpectedEvent::TransactionalProducerCreated(expected_id),
            AdapterEvent::TransactionalProducerCreated {
                producer_id: actual,
            },
        )
        | (
            ExpectedEvent::TransactionalProducerClosed(expected_id),
            AdapterEvent::TransactionalProducerClosed {
                producer_id: actual,
            },
        ) => return Some(identity_result(expected_id == actual, event, expected)),
        (
            ExpectedEvent::TransactionCompleted { operation_ids, .. },
            AdapterEvent::OperationAccepted { operation_id }
            | AdapterEvent::OperationRejected { operation_id, .. }
            | AdapterEvent::OperationTerminal { operation_id, .. },
        ) => operation_ids.contains(operation_id),
        (
            ExpectedEvent::TransactionCompleted { transaction_id, .. },
            AdapterEvent::TransactionCompleted {
                transaction_id: actual,
                ..
            },
        )
        | (
            ExpectedEvent::TransactionFenceCompleted { transaction_id, .. },
            AdapterEvent::TransactionFenceCompleted {
                transaction_id: actual,
                ..
            },
        ) => return Some(identity_result(transaction_id == actual, event, expected)),
        (
            ExpectedEvent::TransactionFenceCompleted { operation_id, .. },
            AdapterEvent::OperationAccepted {
                operation_id: actual,
            }
            | AdapterEvent::OperationRejected {
                operation_id: actual,
                ..
            }
            | AdapterEvent::OperationTerminal {
                operation_id: actual,
                ..
            },
        ) => operation_id == actual,
        (
            ExpectedEvent::TransactionFenceCompleted {
                replacement_producer_id,
                ..
            },
            AdapterEvent::TransactionalProducerCreated {
                producer_id: actual,
            },
        ) => replacement_producer_id == actual,
        _ => return None,
    };
    Some(if matches {
        Ok(EventDisposition::Continue)
    } else {
        Err(identity_mismatch(event, expected))
    })
}

pub(super) fn same_event_family(expected: &ExpectedEvent, event: &AdapterEvent) -> bool {
    if same_admin_event_family(expected, event) {
        return true;
    }
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
                ExpectedEvent::TransactionalProducerCreated(_),
                AdapterEvent::TransactionalProducerCreated { .. }
            )
            | (
                ExpectedEvent::TransactionCompleted { .. },
                AdapterEvent::OperationAccepted { .. }
                    | AdapterEvent::OperationRejected { .. }
                    | AdapterEvent::OperationTerminal { .. }
                    | AdapterEvent::TransactionCompleted { .. }
            )
            | (
                ExpectedEvent::TransactionFenceCompleted { .. },
                AdapterEvent::OperationAccepted { .. }
                    | AdapterEvent::OperationRejected { .. }
                    | AdapterEvent::OperationTerminal { .. }
                    | AdapterEvent::TransactionalProducerCreated { .. }
                    | AdapterEvent::TransactionFenceCompleted { .. }
            )
            | (
                ExpectedEvent::TransactionalProducerClosed(_),
                AdapterEvent::TransactionalProducerClosed { .. }
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

fn identity_result(
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

fn identity_mismatch(event: &AdapterEvent, expected: &ExpectedEvent) -> RunFailure {
    RunFailure::protocol(
        "event_identity_mismatch",
        format!("event {event:?} does not match expected {expected:?}"),
    )
}
