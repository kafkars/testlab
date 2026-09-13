//! Direct assignment verification preserves exact caller-selected assignment input.

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
        .filter(|(_, _, command)| is_assignment(command))
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
        "CONS-030",
        format!(
            "direct assignment expected {} exact ordered consumer, topic-partition, command-kind, and batch-timeout command(s)",
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
        ScenarioAction::AssignBeginning {
            consumer_id,
            topic,
            partition,
        } => Some(AdapterCommand::AssignBeginning {
            consumer_id: consumer_id.clone(),
            topic: topic.clone(),
            partition: *partition,
        }),
        ScenarioAction::AssignBeginningBatch(action) => Some(AdapterCommand::AssignBeginningBatch(
            testlab_schema::AssignBeginningBatchCommand {
                consumer_id: action.consumer_id.clone(),
                partitions: action.partitions.clone(),
                timeout_ms: action.timeout_ms,
            },
        )),
        _ => None,
    }
}

fn is_assignment(command: &AdapterCommand) -> bool {
    matches!(
        command,
        AdapterCommand::AssignBeginning { .. } | AdapterCommand::AssignBeginningBatch(_)
    )
}

#[cfg(test)]
#[path = "assigned_consumer_assignment_command_test.rs"]
mod tests;
