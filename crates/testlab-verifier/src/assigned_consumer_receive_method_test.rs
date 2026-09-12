//! Immediate assigned-consumer receive tests reject observer substitution.

use testlab_schema::{
    AdapterCommand, AssignedConsumerReceiveMethod, HistoryEntry, Scenario, ScenarioAction,
};

use crate::index::HistoryIndex;
use crate::verify_fixture::command;

#[test]
fn exact_immediate_batch_command_passes() {
    let (scenario, history) = fixture(AssignedConsumerReceiveMethod::TryTakeBatch);

    assert!(violations(&scenario, &history).is_empty());
}

#[test]
fn waiting_or_duplicate_receive_command_fails() {
    let (scenario, history) = fixture(AssignedConsumerReceiveMethod::Recv);
    assert!(has_contract(&violations(&scenario, &history), "CONS-015"));

    let (scenario, mut history) = fixture(AssignedConsumerReceiveMethod::TryTakeBatch);
    history.push(history[0].clone());
    assert!(has_contract(&violations(&scenario, &history), "CONS-015"));
}

fn fixture(method: AssignedConsumerReceiveMethod) -> (Scenario, Vec<HistoryEntry>) {
    let scenario: Scenario = toml::from_str(include_str!(
        "../../../scenarios/kafka/assigned-consumer-immediate-batch.toml"
    ))
    .unwrap_or_else(|error| panic!("immediate-batch scenario: {error}"));
    let command_value = scenario
        .steps
        .iter()
        .find_map(|step| match &step.action {
            ScenarioAction::Receive {
                consumer_id,
                method: AssignedConsumerReceiveMethod::TryTakeBatch,
                receive_id,
                timeout_ms,
                ..
            } => Some(AdapterCommand::Receive {
                consumer_id: consumer_id.clone(),
                method,
                receive_id: receive_id.clone(),
                timeout_ms: *timeout_ms,
            }),
            _ => None,
        })
        .unwrap_or_else(|| panic!("immediate-batch action missing"));
    (scenario, vec![command(0, command_value)])
}

fn violations(scenario: &Scenario, history: &[HistoryEntry]) -> Vec<testlab_schema::Violation> {
    let index = HistoryIndex::build(history);
    let mut violations = Vec::new();
    crate::assigned_consumer_receive_method::verify(scenario, &index, &mut violations);
    violations
}

fn has_contract(violations: &[testlab_schema::Violation], contract: &str) -> bool {
    violations
        .iter()
        .any(|violation| violation.contract_id.as_str() == contract)
}
