//! Producer cancellation tests cover every public stage and invalid outcome regression.

use testlab_schema::{
    AdapterCommand, AdapterEvent, ProducerCancellationCompletion, ProducerCancellationOutcome,
    Scenario, ScenarioAction, TerminalStatus,
};

use crate::index::HistoryIndex;
use crate::verify_fixture::{command, event};

#[test]
fn cancellation_outcomes_are_monotonic_with_terminal_truth() {
    assert!(
        verify(
            [
                ProducerCancellationOutcome::CancelledNotSent,
                ProducerCancellationOutcome::AlreadyTerminal,
            ],
            TerminalStatus::DefinitelyNotSent,
            Some("cancelled"),
        )
        .is_empty()
    );
    assert!(
        verify(
            [
                ProducerCancellationOutcome::TooLate,
                ProducerCancellationOutcome::AlreadyTerminal,
            ],
            TerminalStatus::Acknowledged,
            None,
        )
        .is_empty()
    );
    assert!(
        verify(
            [
                ProducerCancellationOutcome::AlreadyTerminal,
                ProducerCancellationOutcome::AlreadyTerminal,
            ],
            TerminalStatus::Acknowledged,
            None,
        )
        .is_empty()
    );
    let violations = verify(
        [
            ProducerCancellationOutcome::TooLate,
            ProducerCancellationOutcome::CancelledNotSent,
        ],
        TerminalStatus::DefinitelyNotSent,
        Some("cancelled"),
    );
    assert!(
        violations
            .iter()
            .any(|value| value.contract_id.as_str() == "PROD-012")
    );
}

fn verify(
    outcomes: [ProducerCancellationOutcome; 2],
    status: TerminalStatus,
    code: Option<&str>,
) -> Vec<testlab_schema::Violation> {
    let scenario = scenario();
    let ScenarioAction::CancelProducerSend(action) = &scenario.steps[3].action else {
        panic!("cancellation action missing");
    };
    let history = vec![
        command(1, AdapterCommand::CancelProducerSend(action.clone())),
        event(
            2,
            AdapterEvent::OperationTerminal {
                operation_id: action.operation_id.clone(),
                status,
                code: code.map(str::to_owned),
                offset: (status == TerminalStatus::Acknowledged).then_some(0),
            },
        ),
        event(
            3,
            AdapterEvent::ProducerCancellationCompleted(ProducerCancellationCompletion {
                operation_id: action.operation_id.clone(),
                outcomes: outcomes.to_vec(),
            }),
        ),
    ];
    let index = HistoryIndex::build(&history);
    let mut violations = Vec::new();
    crate::producer_cancellation::verify(&scenario, &index, &mut violations);
    violations
}

fn scenario() -> Scenario {
    toml::from_str(include_str!(
        "../../../scenarios/kafka/producer-cancellation.toml"
    ))
    .unwrap_or_else(|error| panic!("parse producer cancellation: {error}"))
}
