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

#[cfg(test)]
mod tests {
    use super::*;
    use testlab_schema::{AdapterEvent, AdminProducersDescription};

    #[test]
    fn admin_abort_accepts_only_its_two_producer_snapshot_identities() {
        let transaction_id = operation("transaction-1");
        let record_id = operation("record-1");
        let expected = ExpectedEvent::TransactionCompleted(TransactionExpectation {
            transaction_id: transaction_id.clone(),
            operation_ids: BTreeSet::from([record_id.clone()]),
            disposition: TransactionDisposition::AdminPartitionAbort,
        });
        for operation_id in [record_id, transaction_id] {
            assert_eq!(
                classify(&expected, &description(operation_id))
                    .unwrap_or_else(|| panic!("producer snapshot classification"))
                    .unwrap_or_else(|error| panic!("producer snapshot identity: {error}")),
                EventDisposition::Continue
            );
        }
        assert!(
            classify(&expected, &description(operation("wrong")))
                .unwrap_or_else(|| panic!("producer snapshot classification"))
                .is_err()
        );
    }

    fn description(operation_id: OperationId) -> AdapterEvent {
        AdapterEvent::ProducersDescribed(AdminProducersDescription {
            operation_id,
            topic: "orders".to_owned(),
            partition: 0,
            producers: Vec::new(),
        })
    }

    fn operation(value: &str) -> OperationId {
        OperationId::new(value).unwrap_or_else(|error| panic!("operation ID: {error}"))
    }
}
