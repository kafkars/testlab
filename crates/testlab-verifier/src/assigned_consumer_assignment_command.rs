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
mod tests {
    use testlab_schema::{AdapterCommand, HistoryEntry, HistoryPayload, Scenario};

    use super::{command, verify};
    use crate::index::HistoryIndex;
    use crate::verify_fixture::command as history_command;

    #[test]
    fn single_assignments_preserve_exact_order_and_coordinates() {
        let scenario = parse("assigned-consumer-beginning-reset.toml");
        let exact = assignment_history(&scenario);
        assert_eq!(exact.len(), 3);
        assert!(violations(&scenario, &exact).is_empty());

        let mut wrong_coordinate = exact.clone();
        let AdapterCommand::AssignBeginning {
            topic, partition, ..
        } = command_mut(&mut wrong_coordinate[0])
        else {
            panic!("single assignment command");
        };
        topic.push_str("-substituted");
        *partition += 1;
        assert_contract(&violations(&scenario, &wrong_coordinate));

        let mut reordered = exact.clone();
        reordered.swap(0, 1);
        assert_contract(&violations(&scenario, &reordered));

        let duplicate_command = exact[0].clone();
        let mut duplicate = exact;
        duplicate.push(duplicate_command);
        assert_contract(&violations(&scenario, &duplicate));
    }

    #[test]
    fn batch_assignment_preserves_kind_partition_order_and_timeout() {
        let scenario = parse("assigned-consumer-multi-partition.toml");
        let exact = assignment_history(&scenario);
        assert_eq!(exact.len(), 1);
        assert!(violations(&scenario, &exact).is_empty());

        let mut wrong_batch = exact.clone();
        let AdapterCommand::AssignBeginningBatch(command) = command_mut(&mut wrong_batch[0]) else {
            panic!("batch assignment command");
        };
        command.partitions.reverse();
        command.timeout_ms += 1;
        assert_contract(&violations(&scenario, &wrong_batch));

        let mut substituted = exact;
        let AdapterCommand::AssignBeginningBatch(batch) = command_mut(&mut substituted[0]).clone()
        else {
            panic!("batch assignment command");
        };
        *command_mut(&mut substituted[0]) = AdapterCommand::AssignBeginning {
            consumer_id: batch.consumer_id,
            topic: batch.partitions[0].topic.clone(),
            partition: batch.partitions[0].partition,
        };
        assert_contract(&violations(&scenario, &substituted));
    }

    fn assignment_history(scenario: &Scenario) -> Vec<HistoryEntry> {
        scenario
            .steps
            .iter()
            .filter_map(|step| command(&step.action))
            .enumerate()
            .map(|(index, command)| history_command(index as u64, command))
            .collect()
    }

    fn parse(name: &str) -> Scenario {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../scenarios/kafka")
            .join(name);
        toml::from_str(
            &std::fs::read_to_string(&path)
                .unwrap_or_else(|error| panic!("read {}: {error}", path.display())),
        )
        .unwrap_or_else(|error| panic!("parse {}: {error}", path.display()))
    }

    fn command_mut(entry: &mut HistoryEntry) -> &mut AdapterCommand {
        let HistoryPayload::HarnessCommand { command: envelope } = &mut entry.payload else {
            panic!("command history entry");
        };
        &mut envelope.command
    }

    fn violations(scenario: &Scenario, history: &[HistoryEntry]) -> Vec<testlab_schema::Violation> {
        let mut violations = Vec::new();
        verify(scenario, &HistoryIndex::build(history), &mut violations);
        violations
    }

    fn assert_contract(violations: &[testlab_schema::Violation]) {
        assert!(
            violations
                .iter()
                .any(|violation| violation.contract_id.as_str() == "CONS-030"),
            "{violations:?}"
        );
    }
}
