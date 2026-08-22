//! Protocol expectations distinguish public outcomes from invalidity.

use std::collections::BTreeSet;

use testlab_schema::{
    AdapterEvent, ConsumerId, OperationId, ProducerId, TerminalStatus, TransactionDisposition,
};

use crate::runner_protocol::{EventDisposition, ExpectedEvent};

#[test]
fn public_client_failure_completes_the_correlated_command() {
    let producer =
        ProducerId::new("producer-1").unwrap_or_else(|error| panic!("producer id: {error}"));
    let disposition = ExpectedEvent::FlushCompleted(producer)
        .classify(&AdapterEvent::CommandFailed {
            code: "backpressure".to_owned(),
            diagnostic: "flush contended".to_owned(),
        })
        .unwrap_or_else(|error| panic!("classify client failure: {error}"));

    assert_eq!(disposition, EventDisposition::Complete);
}

#[test]
fn batch_waits_for_explicit_completion_after_known_operation_events() {
    let producer = id(ProducerId::new("producer-1"));
    let operation = id(OperationId::new("op-1"));
    let expected = ExpectedEvent::BatchCompleted {
        producer_id: producer.clone(),
        operation_ids: BTreeSet::from([operation.clone()]),
    };

    assert_eq!(
        expected
            .classify(&AdapterEvent::OperationAccepted {
                operation_id: operation.clone(),
            })
            .unwrap_or_else(|error| panic!("accepted batch operation: {error}")),
        EventDisposition::Continue
    );
    assert_eq!(
        expected
            .classify(&AdapterEvent::OperationTerminal {
                operation_id: operation,
                status: TerminalStatus::Acknowledged,
                code: None,
                offset: Some(0),
            })
            .unwrap_or_else(|error| panic!("terminal batch operation: {error}")),
        EventDisposition::Continue
    );
    assert_eq!(
        expected
            .classify(&AdapterEvent::BatchCompleted {
                producer_id: producer,
            })
            .unwrap_or_else(|error| panic!("completed batch: {error}")),
        EventDisposition::Complete
    );
}

#[test]
fn receive_completion_requires_the_exact_receive_identity() {
    let receive = id(OperationId::new("receive-1"));
    let expected = ExpectedEvent::ReceiveCompleted(receive.clone());

    assert_eq!(
        expected
            .classify(&AdapterEvent::ReceiveCompleted {
                receive_id: receive,
                records: Vec::new(),
            })
            .unwrap_or_else(|error| panic!("classify receive: {error}")),
        EventDisposition::Complete
    );

    let consumer = id(ConsumerId::new("consumer-1"));
    assert!(
        ExpectedEvent::AssignedConsumerClosed(consumer.clone())
            .classify(&AdapterEvent::AssignedConsumerClosed {
                consumer_id: consumer,
            })
            .is_ok()
    );
}

#[test]
fn group_receive_completion_requires_commit_event_identity() {
    let receive = id(OperationId::new("receive-group-1"));
    assert_eq!(
        ExpectedEvent::GroupReceiveCompleted(receive.clone())
            .classify(&AdapterEvent::GroupReceiveCompleted {
                receive_id: receive,
                records: Vec::new(),
                committed: false,
                group_epoch: None,
            })
            .unwrap_or_else(|error| panic!("classify group receive: {error}")),
        EventDisposition::Complete
    );
}

#[test]
fn admin_completion_requires_exact_operation_and_topic() {
    let operation_id = id(OperationId::new("admin-create-1"));
    assert_eq!(
        ExpectedEvent::TopicCreated {
            operation_id: operation_id.clone(),
            topic: "orders".to_owned(),
        }
        .classify(&AdapterEvent::TopicCreated {
            operation_id,
            topic: "orders".to_owned(),
        })
        .unwrap_or_else(|error| panic!("classify admin completion: {error}")),
        EventDisposition::Complete
    );
}

#[test]
fn transaction_waits_for_exact_disposition_identity() {
    let operation_id = id(OperationId::new("transaction-record-1"));
    let transaction_id = id(OperationId::new("transaction-1"));
    let expected = ExpectedEvent::TransactionCompleted {
        transaction_id: transaction_id.clone(),
        operation_ids: BTreeSet::from([operation_id.clone()]),
    };

    assert_eq!(
        expected
            .classify(&AdapterEvent::OperationTerminal {
                operation_id,
                status: TerminalStatus::TransactionStaged,
                code: None,
                offset: Some(0),
            })
            .unwrap_or_else(|error| panic!("classify transaction stage: {error}")),
        EventDisposition::Continue
    );
    assert_eq!(
        expected
            .classify(&AdapterEvent::TransactionCompleted {
                transaction_id,
                disposition: TransactionDisposition::Abort,
            })
            .unwrap_or_else(|error| panic!("classify transaction completion: {error}")),
        EventDisposition::Complete
    );
}

#[test]
fn transaction_fence_waits_for_replacement_and_exact_result_identity() {
    let operation_id = id(OperationId::new("fenced-record-1"));
    let transaction_id = id(OperationId::new("fenced-transaction-1"));
    let replacement = id(ProducerId::new("replacement-1"));
    let expected = ExpectedEvent::TransactionFenceCompleted {
        transaction_id: transaction_id.clone(),
        operation_id: operation_id.clone(),
        replacement_producer_id: replacement.clone(),
    };

    assert_eq!(
        expected
            .classify(&AdapterEvent::OperationAccepted { operation_id })
            .unwrap_or_else(|error| panic!("classify fenced stage: {error}")),
        EventDisposition::Continue
    );
    assert_eq!(
        expected
            .classify(&AdapterEvent::TransactionalProducerCreated {
                producer_id: replacement,
            })
            .unwrap_or_else(|error| panic!("classify replacement: {error}")),
        EventDisposition::Continue
    );
    assert_eq!(
        expected
            .classify(&AdapterEvent::TransactionFenceCompleted {
                transaction_id,
                commit_error_code: Some("fenced".to_owned()),
            })
            .unwrap_or_else(|error| panic!("classify fence completion: {error}")),
        EventDisposition::Complete
    );
}

fn id<T, E>(result: Result<T, E>) -> T
where
    E: std::fmt::Display,
{
    result.unwrap_or_else(|error| panic!("fixture id: {error}"))
}
