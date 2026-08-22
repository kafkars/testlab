//! Protocol expectations distinguish public outcomes from invalidity.

use std::collections::BTreeSet;

use testlab_schema::{AdapterEvent, OperationId, ProducerId, TerminalStatus};

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

fn id<T, E>(result: Result<T, E>) -> T
where
    E: std::fmt::Display,
{
    result.unwrap_or_else(|error| panic!("fixture id: {error}"))
}
