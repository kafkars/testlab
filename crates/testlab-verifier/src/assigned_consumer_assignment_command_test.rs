//! Tests for the assigned consumer assignment command contract.

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
