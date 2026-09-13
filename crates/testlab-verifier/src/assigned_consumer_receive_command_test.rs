//! Tests for the assigned consumer receive command contract.

use testlab_schema::{
    AdapterCommand, AssignedConsumerReceiveMethod, ConsumerId, HistoryEntry, HistoryPayload,
    OperationId, Scenario,
};

use super::{command, verify};
use crate::index::HistoryIndex;
use crate::verify_fixture::command as history_command;

#[test]
fn waiting_receive_preserves_every_exact_request_field_once() {
    let scenario = parse("assigned-consumer-round-trip.toml");
    let exact = receive_history(&scenario);
    assert_eq!(exact.len(), 1);
    assert!(violations(&scenario, &exact).is_empty());

    let mut wrong_identity = exact.clone();
    let AdapterCommand::Receive {
        consumer_id,
        receive_id,
        ..
    } = command_mut(&mut wrong_identity[0])
    else {
        panic!("assigned receive command");
    };
    *consumer_id = ConsumerId::new("substituted-consumer")
        .unwrap_or_else(|error| panic!("consumer ID: {error}"));
    *receive_id = OperationId::new("substituted-receive")
        .unwrap_or_else(|error| panic!("receive ID: {error}"));
    assert_contract(&violations(&scenario, &wrong_identity));

    let mut wrong_observer = exact.clone();
    let AdapterCommand::Receive {
        method, timeout_ms, ..
    } = command_mut(&mut wrong_observer[0])
    else {
        panic!("assigned receive command");
    };
    *method = AssignedConsumerReceiveMethod::TryTakeBatch;
    *timeout_ms += 1;
    assert_contract(&violations(&scenario, &wrong_observer));

    let duplicate_command = exact[0].clone();
    let mut duplicate = exact;
    duplicate.push(duplicate_command);
    assert_contract(&violations(&scenario, &duplicate));
}

#[test]
fn immediate_receive_also_has_one_exact_command() {
    let scenario = parse("assigned-consumer-immediate-batch.toml");
    let history = receive_history(&scenario);
    assert_eq!(history.len(), 1);
    assert!(violations(&scenario, &history).is_empty());
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
            .any(|violation| violation.contract_id.as_str() == "CONS-031"),
        "{violations:?}"
    );
}
