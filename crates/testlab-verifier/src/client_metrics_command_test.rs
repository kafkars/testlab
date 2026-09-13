//! Tests for the client metrics command contract.

use testlab_schema::{
    AdapterCommand, ClientId, HistoryEntry, HistoryPayload, ObserveClientMetricsCommand,
    OperationId, Scenario, ScenarioAction, StepId,
};

use super::{command, verify};
use crate::index::HistoryIndex;
use crate::verify_fixture::command as history_command;

#[test]
fn repeated_metrics_requests_preserve_exact_order_and_identities() {
    let scenario = repeated_scenario();
    let exact = metrics_history(&scenario);
    assert_eq!(exact.len(), 2);
    assert!(violations(&scenario, &exact).is_empty());

    let mut wrong_identity = exact.clone();
    let command = command_mut(&mut wrong_identity[0]);
    command.client_id =
        ClientId::new("substituted-client").unwrap_or_else(|error| panic!("client ID: {error}"));
    command.operation_id = OperationId::new("substituted-operation")
        .unwrap_or_else(|error| panic!("operation ID: {error}"));
    assert_contract(&violations(&scenario, &wrong_identity));

    let mut reordered = exact.clone();
    reordered.swap(0, 1);
    assert_contract(&violations(&scenario, &reordered));

    let mut missing = exact.clone();
    missing.pop();
    assert_contract(&violations(&scenario, &missing));

    let duplicate_command = exact[0].clone();
    let mut duplicate = exact;
    duplicate.push(duplicate_command);
    assert_contract(&violations(&scenario, &duplicate));
}

fn repeated_scenario() -> Scenario {
    let mut scenario = parse("client-metrics-producer.toml");
    let index = scenario
        .steps
        .iter()
        .position(|step| matches!(&step.action, ScenarioAction::ObserveClientMetrics(_)))
        .unwrap_or_else(|| panic!("metrics action"));
    let mut repeated = scenario.steps[index].clone();
    repeated.id =
        StepId::new("observe-metrics-again").unwrap_or_else(|error| panic!("step ID: {error}"));
    let ScenarioAction::ObserveClientMetrics(action) = &mut repeated.action else {
        panic!("metrics action");
    };
    action.operation_id = OperationId::new("metrics-snapshot-again")
        .unwrap_or_else(|error| panic!("operation ID: {error}"));
    scenario.steps.insert(index + 1, repeated);
    scenario
}

fn metrics_history(scenario: &Scenario) -> Vec<HistoryEntry> {
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

fn command_mut(entry: &mut HistoryEntry) -> &mut ObserveClientMetricsCommand {
    let HistoryPayload::HarnessCommand { command: envelope } = &mut entry.payload else {
        panic!("command history entry");
    };
    let AdapterCommand::ObserveClientMetrics(command) = &mut envelope.command else {
        panic!("client metrics command");
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
            .any(|violation| violation.contract_id.as_str() == "METRICS-004"),
        "{violations:?}"
    );
}
