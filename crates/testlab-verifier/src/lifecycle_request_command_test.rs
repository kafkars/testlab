//! Tests for the lifecycle request command contract.

use testlab_schema::{AdapterCommand, ClientId, HistoryEntry, HistoryPayload, Scenario};

use super::{command, verify};
use crate::index::HistoryIndex;
use crate::verify_fixture::command as history_command;

#[test]
fn repeated_readiness_flush_and_terminals_preserve_exact_stream() {
    let scenario = parse("producer-repeated-readiness-flush.toml");
    let exact = lifecycle_history(&scenario);
    assert_eq!(exact.len(), 9);
    assert!(violations(&scenario, &exact).is_empty());

    let mut wrong_identity = exact.clone();
    let AdapterCommand::AwaitClientReady { client_id } = command_mut(&mut wrong_identity[0]) else {
        panic!("client readiness command");
    };
    *client_id =
        ClientId::new("substituted-client").unwrap_or_else(|error| panic!("client ID: {error}"));
    assert_contract(&violations(&scenario, &wrong_identity));

    let mut reordered = exact.clone();
    reordered.swap(0, 2);
    assert_contract(&violations(&scenario, &reordered));

    let mut missing = exact.clone();
    missing.remove(1);
    assert_contract(&violations(&scenario, &missing));

    let duplicate_command = exact[0].clone();
    let mut duplicate = exact;
    duplicate.push(duplicate_command);
    assert_contract(&violations(&scenario, &duplicate));
}

#[test]
fn consumer_and_transaction_terminals_are_included() {
    for name in [
        "assigned-consumer-round-trip.toml",
        "classic-group-round-trip.toml",
        "transaction-multi-record-commit.toml",
        "admin-remove-static-group-members.toml",
    ] {
        let scenario = parse(name);
        let history = lifecycle_history(&scenario);
        assert!(!history.is_empty(), "{name}");
        assert!(violations(&scenario, &history).is_empty(), "{name}");
    }
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
            .any(|violation| violation.contract_id.as_str() == "LIFE-017"),
        "{violations:?}"
    );
}
