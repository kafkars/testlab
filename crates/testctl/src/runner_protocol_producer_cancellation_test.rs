//! Producer cancellation protocol tests pin correlated intermediate and final events.

use testlab_schema::{
    AdapterEvent, OperationId, ProducerCancellationCompletion, ProducerCancellationOutcome,
    TerminalStatus,
};

use crate::runner_protocol::{EventDisposition, ExpectedEvent};

#[test]
fn cancellation_waits_through_terminal_for_exact_completion() {
    let operation_id = operation("cancel-1");
    let expected = ExpectedEvent::ProducerCancellationCompleted(operation_id.clone());
    for event in [
        AdapterEvent::OperationAccepted {
            operation_id: operation_id.clone(),
        },
        AdapterEvent::OperationTerminal {
            operation_id: operation_id.clone(),
            status: TerminalStatus::DefinitelyNotSent,
            code: Some("cancelled".to_owned()),
            offset: None,
        },
    ] {
        assert_eq!(
            expected
                .classify(&event)
                .unwrap_or_else(|error| panic!("classify cancellation event: {error}")),
            EventDisposition::Continue
        );
    }
    let completion = AdapterEvent::ProducerCancellationCompleted(ProducerCancellationCompletion {
        operation_id,
        outcomes: vec![
            ProducerCancellationOutcome::CancelledNotSent,
            ProducerCancellationOutcome::AlreadyTerminal,
        ],
    });
    assert_eq!(
        expected
            .classify(&completion)
            .unwrap_or_else(|error| panic!("classify cancellation completion: {error}")),
        EventDisposition::Complete
    );
}

#[test]
fn cancellation_rejects_foreign_operation_identity() {
    let expected = ExpectedEvent::ProducerCancellationCompleted(operation("cancel-1"));
    assert!(
        expected
            .classify(&AdapterEvent::OperationAccepted {
                operation_id: operation("cancel-2"),
            })
            .is_err()
    );
}

fn operation(value: &str) -> OperationId {
    OperationId::new(value).unwrap_or_else(|error| panic!("operation id: {error}"))
}
