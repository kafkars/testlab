//! Client metrics preserve every exact expectation-free observation request.

use testlab_schema::{
    AdapterCommand, ObserveClientMetricsCommand, Scenario, ScenarioAction, Violation,
};

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
        .filter(|(_, _, command)| matches!(command, AdapterCommand::ObserveClientMetrics(_)))
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
        "METRICS-004",
        format!(
            "client metrics expected {} exact ordered client and operation command(s)",
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
    let ScenarioAction::ObserveClientMetrics(action) = action else {
        return None;
    };
    Some(AdapterCommand::ObserveClientMetrics(
        ObserveClientMetricsCommand {
            client_id: action.client_id.clone(),
            operation_id: action.operation_id.clone(),
        },
    ))
}

#[cfg(test)]
#[path = "client_metrics_command_test.rs"]
mod tests;
