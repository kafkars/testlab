//! Assigned receives preserve the exact public observer request.

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
        .filter(|(_, _, command)| matches!(command, AdapterCommand::Receive { .. }))
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
        "CONS-031",
        format!(
            "assigned receive expected {} exact ordered consumer, observer, identity, and timeout command(s)",
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
    let ScenarioAction::Receive {
        consumer_id,
        method,
        receive_id,
        timeout_ms,
        ..
    } = action
    else {
        return None;
    };
    Some(AdapterCommand::Receive {
        consumer_id: consumer_id.clone(),
        method: *method,
        receive_id: receive_id.clone(),
        timeout_ms: *timeout_ms,
    })
}

#[cfg(test)]
#[path = "assigned_consumer_receive_command_test.rs"]
mod tests;
