//! Group processing acknowledgement tests reject timing-plan substitution.

use testlab_schema::{AdapterCommand, HistoryEntry, Scenario, ScenarioAction};

use crate::index::HistoryIndex;
use crate::verify_fixture::command;

#[test]
fn exact_processing_acknowledgement_command_passes() {
    let (scenario, history) = fixture(3_000);
    assert!(violations(&scenario, &history).is_empty());
}

#[test]
fn omitted_or_duplicate_processing_acknowledgement_command_fails() {
    let (scenario, history) = fixture(0);
    assert!(has_contract(&violations(&scenario, &history), "CONS-019"));

    let (scenario, mut history) = fixture(3_000);
    history.push(history[0].clone());
    assert!(has_contract(&violations(&scenario, &history), "CONS-019"));
}

fn fixture(delay_ms: u64) -> (Scenario, Vec<HistoryEntry>) {
    let scenario: Scenario = toml::from_str(include_str!(
        "../../../scenarios/kafka/classic-group-processing-acknowledgement.toml"
    ))
    .unwrap_or_else(|error| panic!("processing acknowledgement scenario: {error}"));
    let command_value = scenario
        .steps
        .iter()
        .find_map(|step| match &step.action {
            ScenarioAction::GroupReceive {
                consumer_id,
                method,
                receive_id,
                timeout_ms,
                ..
            } => Some(AdapterCommand::GroupReceive {
                consumer_id: consumer_id.clone(),
                method: *method,
                receive_id: receive_id.clone(),
                processing_acknowledgement_delay_ms: delay_ms,
                timeout_ms: *timeout_ms,
            }),
            _ => None,
        })
        .unwrap_or_else(|| panic!("processing acknowledgement action missing"));
    (scenario, vec![command(0, command_value)])
}

fn violations(scenario: &Scenario, history: &[HistoryEntry]) -> Vec<testlab_schema::Violation> {
    let index = HistoryIndex::build(history);
    let mut violations = Vec::new();
    crate::group_processing_acknowledgement::verify(scenario, &index, &mut violations);
    violations
}

fn has_contract(violations: &[testlab_schema::Violation], contract: &str) -> bool {
    violations
        .iter()
        .any(|violation| violation.contract_id.as_str() == contract)
}
