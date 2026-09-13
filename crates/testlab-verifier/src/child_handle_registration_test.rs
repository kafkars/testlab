//! Tests for the child handle registration contract.

use testlab_schema::{
    AdapterCommand, ChildHandleOwnership, HistoryEntry, HistoryPayload, Scenario,
};

use super::{assigned_command, producer_command, verify};
use crate::index::HistoryIndex;
use crate::verify_fixture::command;

#[test]
fn exact_independent_producer_registrations_are_required_once() {
    let scenario = parse("producer-sibling-close-isolation.toml");
    let exact = creation_history(&scenario);
    assert!(violations(&scenario, &exact).is_empty());

    let mut wrong_owner = exact.clone();
    let AdapterCommand::CreateProducer { ownership, .. } = command_mut(&mut wrong_owner[0]) else {
        panic!("producer command kind");
    };
    *ownership = ChildHandleOwnership::Shared;
    assert_contract(&violations(&scenario, &wrong_owner), "PROD-020");

    let mut substituted = exact.clone();
    let AdapterCommand::CreateProducer {
        client_id,
        producer_id,
        ..
    } = command_mut(&mut substituted[0]).clone()
    else {
        panic!("producer command kind");
    };
    *command_mut(&mut substituted[0]) = AdapterCommand::CreateTransactionalProducer {
        client_id,
        producer_id,
        transactional_id: "substituted".to_owned(),
        transaction_timeout_ms: 30_000,
        initialization_timeout_ms: 30_000,
    };
    assert_contract(&violations(&scenario, &substituted), "PROD-020");

    let mut duplicate = exact.clone();
    duplicate.push(exact[0].clone());
    assert_contract(&violations(&scenario, &duplicate), "PROD-020");
}

#[test]
fn exact_independent_assigned_registrations_are_required_once() {
    let scenario = parse("assigned-consumer-independent-cursors.toml");
    let exact = creation_history(&scenario);
    assert!(violations(&scenario, &exact).is_empty());

    let mut wrong_owner = exact.clone();
    let AdapterCommand::CreateAssignedConsumer { ownership, .. } = command_mut(&mut wrong_owner[0])
    else {
        panic!("assigned-consumer command kind");
    };
    *ownership = ChildHandleOwnership::Shared;
    assert_contract(&violations(&scenario, &wrong_owner), "CONS-029");

    let mut substituted = exact.clone();
    let AdapterCommand::CreateAssignedConsumer {
        client_id,
        consumer_id,
        ..
    } = command_mut(&mut substituted[0]).clone()
    else {
        panic!("assigned-consumer command kind");
    };
    *command_mut(&mut substituted[0]) = AdapterCommand::CreateGroupConsumer {
        client_id,
        consumer_id,
        group_id: "substituted".to_owned(),
        topics: vec!["records".to_owned()],
        protocol: testlab_schema::GroupProtocol::Classic,
        configuration: None,
    };
    assert_contract(&violations(&scenario, &substituted), "CONS-029");

    let mut duplicate = exact.clone();
    duplicate.push(exact[0].clone());
    assert_contract(&violations(&scenario, &duplicate), "CONS-029");
}

#[test]
fn shared_registrations_are_also_exact() {
    for scenario in [
        parse("producer-round-trip.toml"),
        parse("assigned-consumer-round-trip.toml"),
    ] {
        let history = creation_history(&scenario);
        assert!(violations(&scenario, &history).is_empty());
    }
}

fn creation_history(scenario: &Scenario) -> Vec<HistoryEntry> {
    scenario
        .steps
        .iter()
        .filter_map(|step| {
            producer_command(&step.action).or_else(|| assigned_command(&step.action))
        })
        .enumerate()
        .map(|(index, command_value)| command(index as u64, command_value))
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

fn assert_contract(violations: &[testlab_schema::Violation], contract: &str) {
    assert!(
        violations
            .iter()
            .any(|violation| violation.contract_id.as_str() == contract),
        "{violations:?}"
    );
}
