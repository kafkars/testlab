//! Repeated public lifecycle requests preserve the exact scenario command stream.

use testlab_schema::{AdapterCommand, Scenario, ScenarioAction, Violation};

use crate::index::HistoryIndex;
use crate::support::violation;

pub(super) fn verify(scenario: &Scenario, index: &HistoryIndex, violations: &mut Vec<Violation>) {
    if index.commands.is_empty() {
        return;
    }
    let expected = scenario
        .steps
        .iter()
        .filter_map(|step| command(&step.action))
        .collect::<Vec<_>>();
    if expected.is_empty() {
        return;
    }
    let actual = index
        .commands
        .iter()
        .filter(|(_, _, command)| is_lifecycle_request(command))
        .collect::<Vec<_>>();
    let exact = actual.len() == expected.len()
        && actual
            .iter()
            .zip(&expected)
            .all(|((_, _, actual), expected)| *actual == *expected);
    if exact {
        return;
    }
    violations.push(violation(
        "LIFE-017",
        format!(
            "lifecycle expected {} exact ordered readiness, flush, close, abandonment, and shutdown command(s)",
            expected.len()
        ),
        None,
        actual
            .into_iter()
            .map(|(sequence, _, _)| format!("history:{sequence}"))
            .collect(),
    ));
}

fn command(action: &ScenarioAction) -> Option<AdapterCommand> {
    match action {
        ScenarioAction::AwaitClientReady { client_id } => Some(AdapterCommand::AwaitClientReady {
            client_id: client_id.clone(),
        }),
        ScenarioAction::Flush { producer_id } => Some(AdapterCommand::Flush {
            producer_id: producer_id.clone(),
        }),
        ScenarioAction::CloseProducer { producer_id } => Some(AdapterCommand::CloseProducer {
            producer_id: producer_id.clone(),
        }),
        ScenarioAction::ShutdownClient { client_id } => Some(AdapterCommand::ShutdownClient {
            client_id: client_id.clone(),
        }),
        ScenarioAction::CloseAssignedConsumer { consumer_id } => {
            Some(AdapterCommand::CloseAssignedConsumer {
                consumer_id: consumer_id.clone(),
            })
        }
        ScenarioAction::CloseGroupConsumer { consumer_id } => {
            Some(AdapterCommand::CloseGroupConsumer {
                consumer_id: consumer_id.clone(),
            })
        }
        ScenarioAction::AbandonGroupConsumer(action) => {
            Some(AdapterCommand::AbandonGroupConsumer(action.clone()))
        }
        ScenarioAction::CloseTransactionalProducer(action) => {
            Some(AdapterCommand::CloseTransactionalProducer {
                producer_id: action.producer_id.clone(),
            })
        }
        _ => None,
    }
}

fn is_lifecycle_request(command: &AdapterCommand) -> bool {
    matches!(
        command,
        AdapterCommand::AwaitClientReady { .. }
            | AdapterCommand::Flush { .. }
            | AdapterCommand::CloseProducer { .. }
            | AdapterCommand::ShutdownClient { .. }
            | AdapterCommand::CloseAssignedConsumer { .. }
            | AdapterCommand::CloseGroupConsumer { .. }
            | AdapterCommand::AbandonGroupConsumer(_)
            | AdapterCommand::CloseTransactionalProducer { .. }
    )
}

#[cfg(test)]
#[path = "lifecycle_request_command_test.rs"]
mod tests;
