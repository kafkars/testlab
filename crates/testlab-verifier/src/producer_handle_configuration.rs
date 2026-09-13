//! Producer construction preserves optional per-handle delivery policy.

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
        .filter(|(_, _, command)| matches!(command, AdapterCommand::CreateProducer { .. }))
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
        "PROD-022",
        format!(
            "producer construction expected {} exact ordered client, producer, ownership, and optional handle-timeout command(s)",
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
    let ScenarioAction::CreateProducer {
        client_id,
        producer_id,
        ownership,
        delivery_timeout_ms,
    } = action
    else {
        return None;
    };
    Some(AdapterCommand::CreateProducer {
        client_id: client_id.clone(),
        producer_id: producer_id.clone(),
        ownership: *ownership,
        delivery_timeout_ms: *delivery_timeout_ms,
    })
}

#[cfg(test)]
#[path = "producer_handle_configuration_test.rs"]
mod tests;
