//! Configured producer clients require one exact policy and public builder selection command.

use testlab_schema::{AdapterCommand, ClientId, Scenario, ScenarioAction, Violation};

use crate::index::HistoryIndex;
use crate::support::violation;

pub(super) fn verify(scenario: &Scenario, index: &HistoryIndex, violations: &mut Vec<Violation>) {
    for step in &scenario.steps {
        let ScenarioAction::CreateConfiguredClient(expected) = &step.action else {
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
                    AdapterCommand::CreateConfiguredClient(actual) if actual == expected
                )
            });
        if exact {
            continue;
        }
        violations.push(violation(
            "PROD-019",
            format!(
                "configured client {} expected one exact producer policy and public configuration method command",
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
#[path = "producer_configuration_method_test.rs"]
mod tests;
