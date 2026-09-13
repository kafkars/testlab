//! Tests for the group receive set command contract.

use testlab_schema::{
    AdapterCommand, ConsumerId, GroupReceiveSetCommand, HistoryEntry, HistoryPayload, OperationId,
    Scenario,
};

use super::{command, verify};
use crate::index::HistoryIndex;
use crate::verify_fixture::command as history_command;

#[test]
fn repeated_receive_sets_preserve_exact_order_and_fields() {
    let scenario = parse("classic-group-membership-ownership.toml");
    let exact = receive_history(&scenario);
    assert_eq!(exact.len(), 2);
    assert!(violations(&scenario, &exact).is_empty());

    let mut wrong_identity = exact.clone();
    let command = command_mut(&mut wrong_identity[0]);
    command.receive_id = OperationId::new("substituted-receive")
        .unwrap_or_else(|error| panic!("receive ID: {error}"));
    assert_contract(&violations(&scenario, &wrong_identity));

    let mut wrong_consumers = exact.clone();
    let command = command_mut(&mut wrong_consumers[0]);
    command.consumer_ids.swap(0, 1);
    assert_contract(&violations(&scenario, &wrong_consumers));

    let mut substituted_consumer = exact.clone();
    let command = command_mut(&mut substituted_consumer[1]);
    command.consumer_ids[0] = ConsumerId::new("substituted-consumer")
        .unwrap_or_else(|error| panic!("consumer ID: {error}"));
    assert_contract(&violations(&scenario, &substituted_consumer));

    let mut wrong_bounds = exact.clone();
    let command = command_mut(&mut wrong_bounds[0]);
    command.record_count += 1;
    command.timeout_ms += 1;
    assert_contract(&violations(&scenario, &wrong_bounds));

    let mut reordered = exact.clone();
    reordered.swap(0, 1);
    assert_contract(&violations(&scenario, &reordered));

    let duplicate_command = exact[0].clone();
    let mut duplicate = exact;
    duplicate.push(duplicate_command);
    assert_contract(&violations(&scenario, &duplicate));
}

fn receive_history(scenario: &Scenario) -> Vec<HistoryEntry> {
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

fn command_mut(entry: &mut HistoryEntry) -> &mut GroupReceiveSetCommand {
    let HistoryPayload::HarnessCommand { command: envelope } = &mut entry.payload else {
        panic!("command history entry");
    };
    let AdapterCommand::GroupReceiveSet(command) = &mut envelope.command else {
        panic!("group receive set command");
    };
    command
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
            .any(|violation| violation.contract_id.as_str() == "CONS-033"),
        "{violations:?}"
    );
}
