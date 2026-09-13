//! Tests for the share lifecycle command contract.

use testlab_schema::{
    AdapterCommand, ConsumerId, HistoryEntry, HistoryPayload, OperationId, Scenario,
};

use super::{command, verify};
use crate::index::HistoryIndex;
use crate::verify_fixture::command as history_command;

#[test]
fn batch_drop_and_close_preserve_exact_order_and_identities() {
    let scenario = parse("share-group-lifecycle.toml");
    let exact = lifecycle_history(&scenario);
    assert_eq!(exact.len(), 2);
    assert!(violations(&scenario, &exact).is_empty());

    let mut wrong_drop = exact.clone();
    let AdapterCommand::DropShareBatch {
        consumer_id,
        receive_id,
    } = command_mut(&mut wrong_drop[0])
    else {
        panic!("Share batch-drop command");
    };
    *consumer_id = ConsumerId::new("substituted-consumer")
        .unwrap_or_else(|error| panic!("consumer ID: {error}"));
    *receive_id = OperationId::new("substituted-receive")
        .unwrap_or_else(|error| panic!("receive ID: {error}"));
    assert_contract(&violations(&scenario, &wrong_drop));

    let mut wrong_close = exact.clone();
    let AdapterCommand::CloseShareConsumer { consumer_id } = command_mut(&mut wrong_close[1])
    else {
        panic!("Share consumer-close command");
    };
    *consumer_id = ConsumerId::new("substituted-consumer")
        .unwrap_or_else(|error| panic!("consumer ID: {error}"));
    assert_contract(&violations(&scenario, &wrong_close));

    let mut reordered = exact.clone();
    reordered.swap(0, 1);
    assert_contract(&violations(&scenario, &reordered));

    let duplicate_command = exact[0].clone();
    let mut duplicate = exact;
    duplicate.push(duplicate_command);
    assert_contract(&violations(&scenario, &duplicate));
}

fn lifecycle_history(scenario: &Scenario) -> Vec<HistoryEntry> {
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
            .any(|violation| violation.contract_id.as_str() == "SHARE-014"),
        "{violations:?}"
    );
}
