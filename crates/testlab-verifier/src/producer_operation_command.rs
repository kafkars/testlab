//! Ordinary producer verification preserves every exact caller-selected request.

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
        .filter(|(_, _, command)| is_producer_operation(command))
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
        "PROD-021",
        format!(
            "ordinary production expected {} exact ordered single or batch command(s)",
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
        ScenarioAction::Send {
            producer_id,
            operation_id,
            method,
            partitioning,
            topic_identity_operation_id,
            record,
        } => Some(AdapterCommand::Send {
            producer_id: producer_id.clone(),
            operation_id: operation_id.clone(),
            method: *method,
            partitioning: *partitioning,
            validate_topic_uuid: topic_identity_operation_id.is_some(),
            record: record.clone(),
        }),
        ScenarioAction::SendBatch {
            producer_id,
            operations,
        } => Some(AdapterCommand::SendBatch {
            producer_id: producer_id.clone(),
            operations: operations.clone(),
        }),
        _ => None,
    }
}

fn is_producer_operation(command: &AdapterCommand) -> bool {
    matches!(
        command,
        AdapterCommand::Send { .. } | AdapterCommand::SendBatch { .. }
    )
}

#[cfg(test)]
#[path = "producer_operation_command_test.rs"]
mod tests;
