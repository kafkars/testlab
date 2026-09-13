//! Baseline clients preserve every exact construction request.

use testlab_schema::{AdapterCommand, CreateClientCommand, Scenario, ScenarioAction, Violation};

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
        .filter(|(_, _, command)| matches!(command, AdapterCommand::CreateClient(_)))
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
        "CLIENT-002",
        format!(
            "client creation expected {} exact ordered identity and cluster-guard command(s)",
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
    let ScenarioAction::CreateClient(action) = action else {
        return None;
    };
    Some(AdapterCommand::CreateClient(CreateClientCommand {
        client_id: action.client_id.clone(),
        expected_cluster_id: action.expected_cluster_id.clone(),
    }))
}

#[cfg(test)]
#[path = "client_creation_command_test.rs"]
mod tests;
