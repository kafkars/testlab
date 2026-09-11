//! Transaction Admin completions preserve operation and caller identities.

use testlab_schema::{AdapterEvent, AdminProducersFenced, AdminTransactionsDescription};

use crate::run_error::RunFailure;
use crate::runner_protocol::{EventDisposition, ExpectedEvent};

pub(super) fn classify(
    expected: &ExpectedEvent,
    event: &AdapterEvent,
) -> Option<Result<EventDisposition, RunFailure>> {
    let matches = match (expected, event) {
        (ExpectedEvent::TransactionsListed(expected), AdapterEvent::TransactionsListed(actual)) => {
            expected == &actual.operation_id
        }
        (
            ExpectedEvent::TransactionsDescribed(operation_id, transactional_ids),
            AdapterEvent::TransactionsDescribed(AdminTransactionsDescription {
                operation_id: actual_operation,
                transactions,
            }),
        ) => {
            operation_id == actual_operation
                && transactions.len() == transactional_ids.len()
                && transactions
                    .iter()
                    .zip(transactional_ids)
                    .all(|(actual, expected)| &actual.transactional_id == expected)
        }
        (
            ExpectedEvent::ProducersFenced(operation_id, transactional_ids),
            AdapterEvent::ProducersFenced(AdminProducersFenced {
                operation_id: actual_operation,
                producers,
                ..
            }),
        ) => {
            operation_id == actual_operation
                && producers.len() == transactional_ids.len()
                && producers
                    .iter()
                    .zip(transactional_ids)
                    .all(|(actual, expected)| &actual.transactional_id == expected)
        }
        _ => return None,
    };
    if matches {
        Some(Ok(EventDisposition::Complete))
    } else {
        Some(Err(RunFailure::protocol(
            "event_identity_mismatch",
            format!("event {event:?} does not match expected {expected:?}"),
        )))
    }
}
