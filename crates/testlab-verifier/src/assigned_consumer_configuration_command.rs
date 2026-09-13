//! Configured assigned consumers require one exact immutable policy command.

use testlab_schema::{AdapterCommand, ClientId, Scenario, ScenarioAction, Violation};

use crate::index::HistoryIndex;
use crate::support::violation;

pub(super) fn verify(scenario: &Scenario, index: &HistoryIndex, violations: &mut Vec<Violation>) {
    for step in &scenario.steps {
        let ScenarioAction::CreateAssignedConsumerClient(expected) = &step.action else {
            continue;
        };
        let relevant = index
            .commands
            .iter()
            .filter(|(_, _, command)| creation_client_id(command) == Some(&expected.client_id))
            .collect::<Vec<_>>();
        let exact = relevant.len() == 1
            && relevant.iter().all(|(_, _, command)| {
                matches!(
                    command,
                    AdapterCommand::CreateAssignedConsumerClient(actual) if actual == expected
                )
            });
        if exact {
            continue;
        }
        violations.push(violation(
            "CONS-027",
            format!(
                "configured assigned-consumer client {} expected one exact immutable policy command",
                expected.client_id
            ),
            None,
            relevant
                .into_iter()
                .map(|(sequence, _, _)| format!("history:{sequence}"))
                .collect(),
        ));
    }
}

fn creation_client_id(command: &AdapterCommand) -> Option<&ClientId> {
    match command {
        AdapterCommand::CreateClient(command) => Some(&command.client_id),
        AdapterCommand::CreateConfiguredClient(action) => Some(&action.client_id),
        AdapterCommand::CreateAssignedConsumerClient(action) => Some(&action.client_id),
        _ => None,
    }
}

#[cfg(test)]
#[path = "assigned_consumer_configuration_command_test.rs"]
mod tests;
