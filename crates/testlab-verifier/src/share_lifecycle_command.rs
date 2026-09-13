//! Share batch drop and consumer close preserve exact linear lifecycle requests.

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
        .filter(|(_, _, command)| is_lifecycle(command))
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
        "SHARE-014",
        format!(
            "Share lifecycle expected {} exact ordered batch-drop and consumer-close command(s)",
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
        ScenarioAction::DropShareBatch {
            consumer_id,
            receive_id,
        } => Some(AdapterCommand::DropShareBatch {
            consumer_id: consumer_id.clone(),
            receive_id: receive_id.clone(),
        }),
        ScenarioAction::CloseShareConsumer { consumer_id, .. } => {
            Some(AdapterCommand::CloseShareConsumer {
                consumer_id: consumer_id.clone(),
            })
        }
        _ => None,
    }
}

fn is_lifecycle(command: &AdapterCommand) -> bool {
    matches!(
        command,
        AdapterCommand::DropShareBatch { .. } | AdapterCommand::CloseShareConsumer { .. }
    )
}

#[cfg(test)]
#[path = "share_lifecycle_command_test.rs"]
mod tests;
