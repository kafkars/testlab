//! Producer cancellation verification preserves public stage uncertainty and monotonicity.

use testlab_schema::{
    ProducerCancellationOutcome, Scenario, ScenarioAction, TerminalStatus, Violation,
};

use crate::index::{HistoryIndex, IndexedProducerCancellation, IndexedTerminal};
use crate::support::violation;

pub(crate) fn verify(scenario: &Scenario, index: &HistoryIndex, violations: &mut Vec<Violation>) {
    for step in &scenario.steps {
        let ScenarioAction::CancelProducerSend(action) = &step.action else {
            continue;
        };
        verify_command(action, index, violations);
        if !index.action_issued(&step.action) {
            continue;
        }
        let completions = index.producer_cancellations.get(&action.operation_id);
        let terminals = index.terminals.get(&action.operation_id);
        let exact = matches!(
            (
                completions.map(Vec::as_slice),
                terminals.map(Vec::as_slice)
            ),
            (Some([completion]), Some([terminal]))
                if completion.history_sequence > terminal.history_sequence
                    && coherent(completion, terminal)
        );
        if !exact {
            let mut evidence = completions
                .into_iter()
                .flatten()
                .map(|value| format!("history:{}", value.history_sequence))
                .collect::<Vec<_>>();
            evidence.extend(
                terminals
                    .into_iter()
                    .flatten()
                    .map(|value| format!("history:{}", value.history_sequence)),
            );
            violations.push(violation(
                "PROD-012",
                "two public cancellation outcomes were not monotonic with terminal delivery truth"
                    .to_owned(),
                Some(action.operation_id.clone()),
                evidence,
            ));
        }
    }
}

fn verify_command(
    action: &testlab_schema::CancelProducerSendCommand,
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) {
    let commands = index
        .commands
        .iter()
        .filter_map(|(sequence, _, command)| match command {
            testlab_schema::AdapterCommand::CancelProducerSend(actual)
                if actual.operation_id == action.operation_id =>
            {
                Some((*sequence, actual))
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    if matches!(commands.as_slice(), [(_, actual)] if *actual == action) {
        return;
    }
    violations.push(violation(
        "PROD-017",
        format!(
            "operation {} observed {} cancellation command(s) without one exact producer method and record",
            action.operation_id,
            commands.len()
        ),
        Some(action.operation_id.clone()),
        commands
            .iter()
            .map(|(sequence, _)| format!("history:{sequence}"))
            .collect(),
    ));
}

fn coherent(completion: &IndexedProducerCancellation, terminal: &IndexedTerminal) -> bool {
    match completion.outcomes.as_slice() {
        [
            ProducerCancellationOutcome::CancelledNotSent,
            ProducerCancellationOutcome::AlreadyTerminal,
        ] => {
            terminal.status == TerminalStatus::DefinitelyNotSent
                && terminal.code.as_deref() == Some("cancelled")
                && terminal.offset.is_none()
        }
        [ProducerCancellationOutcome::TooLate, second] => matches!(
            second,
            ProducerCancellationOutcome::TooLate | ProducerCancellationOutcome::AlreadyTerminal
        ),
        [
            ProducerCancellationOutcome::AlreadyTerminal,
            ProducerCancellationOutcome::AlreadyTerminal,
        ] => true,
        _ => false,
    }
}
