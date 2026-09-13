//! Transaction event classification preserves exact terminal and snapshot identities.

use std::collections::BTreeSet;

use testlab_schema::{AdapterEvent, OperationId, TransactionDisposition};

use crate::run_error::RunFailure;
use crate::runner_protocol::{EventDisposition, ExpectedEvent};
use crate::runner_protocol_identity::{identity_mismatch, identity_result};

#[derive(Clone, Debug)]
pub(crate) struct TransactionExpectation {
    pub(crate) transaction_id: OperationId,
    pub(crate) operation_ids: BTreeSet<OperationId>,
    pub(crate) disposition: TransactionDisposition,
}

pub(super) fn classify(
    expected: &ExpectedEvent,
    event: &AdapterEvent,
) -> Option<Result<EventDisposition, RunFailure>> {
    let matches = match (expected, event) {
        (
            ExpectedEvent::TransactionalProducerCreated(expected_id),
            AdapterEvent::TransactionalProducerCreated(observation),
        ) => {
            return Some(identity_result(
                expected_id == &observation.producer_id,
                event,
                expected,
            ));
        }
        (
            ExpectedEvent::TransactionalProducerClosed(expected_id),
            AdapterEvent::TransactionalProducerClosed {
                producer_id: actual,
            },
        ) => {
            return Some(identity_result(expected_id == actual, event, expected));
        }
        (
            ExpectedEvent::TransactionCompleted(transaction),
            AdapterEvent::OperationAccepted { operation_id }
            | AdapterEvent::OperationRejected { operation_id, .. }
            | AdapterEvent::OperationTerminal { operation_id, .. },
        ) => transaction.operation_ids.contains(operation_id),
        (
            ExpectedEvent::TransactionCompleted(transaction),
            AdapterEvent::TransactionCompleted {
                transaction_id: actual_id,
                disposition: actual_disposition,
                ..
            },
        ) => {
            return Some(identity_result(
                &transaction.transaction_id == actual_id
                    && &transaction.disposition == actual_disposition,
                event,
                expected,
            ));
        }
        (
            ExpectedEvent::TransactionCompleted(transaction),
            AdapterEvent::TransactionalTransformCompleted(actual),
        ) => {
            return Some(identity_result(
                transaction.transaction_id == actual.transaction_id
                    && transaction.disposition == actual.disposition,
                event,
                expected,
            ));
        }
        (
            ExpectedEvent::TransactionCompleted(transaction),
            AdapterEvent::ProducersDescribed(actual),
        ) if transaction.disposition == TransactionDisposition::AdminPartitionAbort => {
            actual.operation_id == transaction.transaction_id
                || transaction.operation_ids.contains(&actual.operation_id)
        }
        (
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
            AdapterEvent::TransactionalProducerCreated(observation),
        ) => replacement_producer_id == &observation.producer_id,
        _ => return None,
    };
    Some(if matches {
        Ok(EventDisposition::Continue)
    } else {
        Err(identity_mismatch(event, expected))
    })
}
