//! Multi-member receives preserve each exact aggregate public request.

use testlab_schema::{AdapterCommand, GroupReceiveSetCommand, Scenario, ScenarioAction, Violation};

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
        .filter(|(_, _, command)| matches!(command, AdapterCommand::GroupReceiveSet(_)))
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
        "CONS-033",
        format!(
            "group receive set expected {} exact ordered identity, consumer set, record count, and timeout command(s)",
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
    let ScenarioAction::GroupReceiveSet(action) = action else {
        return None;
    };
    Some(AdapterCommand::GroupReceiveSet(GroupReceiveSetCommand {
        receive_id: action.receive_id.clone(),
        consumer_ids: action.consumer_ids.clone(),
        record_count: action.expected_operation_ids.len(),
        timeout_ms: action.timeout_ms,
    }))
}

#[cfg(test)]
#[path = "group_receive_set_command_test.rs"]
mod tests;
