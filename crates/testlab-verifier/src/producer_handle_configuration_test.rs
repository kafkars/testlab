//! Tests for the producer handle configuration contract.

use testlab_schema::{AdapterCommand, HistoryEntry, HistoryPayload, Scenario};

use super::{command, verify};
use crate::index::HistoryIndex;
use crate::verify_fixture::command as history_command;

#[test]
fn producer_handle_timeout_and_inheritance_are_exact() {
    let scenario = parse();
    let exact = history(&scenario);
    assert_eq!(exact.len(), 1);
    assert!(violations(&scenario, &exact).is_empty());

    let mut omitted = exact.clone();
    let AdapterCommand::CreateProducer {
        delivery_timeout_ms,
        ..
    } = command_mut(&mut omitted[0])
    else {
        panic!("producer creation command");
    };
    *delivery_timeout_ms = None;
    assert_contract(&violations(&scenario, &omitted));

    let mut changed = exact.clone();
    let AdapterCommand::CreateProducer {
        delivery_timeout_ms,
        ..
    } = command_mut(&mut changed[0])
    else {
        panic!("producer creation command");
    };
    *delivery_timeout_ms = Some(15_001);
    assert_contract(&violations(&scenario, &changed));

    let duplicate = exact[0].clone();
    let mut duplicated = exact;
    duplicated.push(duplicate);
    assert_contract(&violations(&scenario, &duplicated));
}

fn history(scenario: &Scenario) -> Vec<HistoryEntry> {
    scenario
        .steps
        .iter()
        .filter_map(|step| command(&step.action))
        .enumerate()
        .map(|(index, command)| history_command(index as u64, command))
        .collect()
}

fn parse() -> Scenario {
    toml::from_str(include_str!(
        "../../../scenarios/kafka/producer-round-trip.toml"
    ))
    .unwrap_or_else(|error| panic!("parse producer round trip: {error}"))
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
            .any(|violation| violation.contract_id.as_str() == "PROD-022"),
        "{violations:?}"
    );
}
