//! Tests for the client creation command contract.

use testlab_schema::{
    AdapterCommand, ClientId, CreateClientCommand, HistoryEntry, HistoryPayload, Scenario,
};

use super::{command, verify};
use crate::index::HistoryIndex;
use crate::verify_fixture::command as history_command;

#[test]
fn rejected_then_successful_creation_preserves_exact_order_and_guards() {
    let scenario = parse("admin-describe-cluster.toml");
    let exact = creation_history(&scenario);
    assert_eq!(exact.len(), 2);
    assert!(violations(&scenario, &exact).is_empty());

    let mut wrong_identity = exact.clone();
    command_mut(&mut wrong_identity[1]).client_id =
        ClientId::new("substituted-client").unwrap_or_else(|error| panic!("client ID: {error}"));
    assert_contract(&violations(&scenario, &wrong_identity));

    let mut omitted_guard = exact.clone();
    command_mut(&mut omitted_guard[1]).expected_cluster_id = None;
    assert_contract(&violations(&scenario, &omitted_guard));

    let mut reordered = exact.clone();
    reordered.swap(0, 1);
    assert_contract(&violations(&scenario, &reordered));

    let duplicate_command = exact[0].clone();
    let mut duplicate = exact;
    duplicate.push(duplicate_command);
    assert_contract(&violations(&scenario, &duplicate));
}

#[test]
fn unguarded_creation_preserves_explicit_absence() {
    let scenario = parse("producer-round-trip.toml");
    let mut history = creation_history(&scenario);
    assert_eq!(history.len(), 1);
    assert!(violations(&scenario, &history).is_empty());
    assert!(command_mut(&mut history[0]).expected_cluster_id.is_none());
}

fn creation_history(scenario: &Scenario) -> Vec<HistoryEntry> {
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

fn command_mut(entry: &mut HistoryEntry) -> &mut CreateClientCommand {
    let HistoryPayload::HarnessCommand { command: envelope } = &mut entry.payload else {
        panic!("command history entry");
    };
    let AdapterCommand::CreateClient(command) = &mut envelope.command else {
        panic!("client creation command");
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
            .any(|violation| violation.contract_id.as_str() == "CLIENT-002"),
        "{violations:?}"
    );
}
