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

#[test]
fn cancellation_command_must_preserve_the_selected_method() {
    let scenario = scenario();
    let mut history = history(
        &scenario,
        [
            ProducerCancellationOutcome::TooLate,
            ProducerCancellationOutcome::AlreadyTerminal,
        ],
        TerminalStatus::Acknowledged,
        None,
    );
    let Some(testlab_schema::HistoryPayload::HarnessCommand { command }) = history
        .iter_mut()
        .find(|entry| {
            matches!(
                &entry.payload,
                testlab_schema::HistoryPayload::HarnessCommand { .. }
            )
        })
        .map(|entry| &mut entry.payload)
    else {
        panic!("cancellation command fixture missing");
    };
    let AdapterCommand::CancelProducerSend(command) = &mut command.command else {
        panic!("cancellation command payload missing");
    };
    command.method = testlab_schema::ProducerSendMethod::Send;

    let index = HistoryIndex::build(&history);
    let mut violations = Vec::new();
    crate::producer_cancellation::verify(&scenario, &index, &mut violations);
    assert!(has(&violations, "PROD-017"));
}

fn verify(
    outcomes: [ProducerCancellationOutcome; 2],
    status: TerminalStatus,
    code: Option<&str>,
) -> Vec<testlab_schema::Violation> {
    let scenario = scenario();
    let history = history(&scenario, outcomes, status, code);
    let index = HistoryIndex::build(&history);
    let mut violations = Vec::new();
    crate::producer_cancellation::verify(&scenario, &index, &mut violations);
    violations
}

fn history(
    scenario: &Scenario,
    outcomes: [ProducerCancellationOutcome; 2],
    status: TerminalStatus,
    code: Option<&str>,
) -> Vec<testlab_schema::HistoryEntry> {
    scenario
        .steps
        .iter()
        .filter_map(|step| match &step.action {
            ScenarioAction::CancelProducerSend(action) => Some(action),
            _ => None,
        })
        .enumerate()
        .flat_map(|(index, action)| {
            let sequence = u64::try_from(index).unwrap_or(u64::MAX) * 3 + 1;
            [
                command(sequence, AdapterCommand::CancelProducerSend(action.clone())),
                event(
                    sequence + 1,
                    AdapterEvent::OperationTerminal {
                        operation_id: action.operation_id.clone(),
                        status,
                        code: code.map(str::to_owned),
                        partition: (status == TerminalStatus::Acknowledged).then_some(0),
                        offset: (status == TerminalStatus::Acknowledged).then_some(0),
                        timestamp_millis: None,
                    },
                ),
                event(
                    sequence + 2,
                    AdapterEvent::ProducerCancellationCompleted(ProducerCancellationCompletion {
                        operation_id: action.operation_id.clone(),
                        outcomes: outcomes.to_vec(),
                    }),
                ),
            ]
        })
        .collect()
}

fn has(violations: &[testlab_schema::Violation], contract: &str) -> bool {
    violations
        .iter()
        .any(|violation| violation.contract_id.as_str() == contract)
}

fn scenario() -> Scenario {
    toml::from_str(include_str!(
        "../../../scenarios/kafka/producer-cancellation.toml"
    ))
    .unwrap_or_else(|error| panic!("parse producer cancellation: {error}"))
}
