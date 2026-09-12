//! Partial checkpoint tests reject processed-prefix substitution.

use testlab_schema::{AdapterCommand, HistoryEntry, Scenario, ScenarioAction};

use crate::index::HistoryIndex;
use crate::verify_fixture::command;

#[test]
fn exact_partial_checkpoint_command_passes() {
    let (scenario, history) = fixture(Some(1));
    assert!(violations(&scenario, &history).is_empty());
}

#[test]
fn full_or_duplicate_checkpoint_command_fails() {
    let (scenario, history) = fixture(None);
    assert!(has_contract(&violations(&scenario, &history), "CONS-020"));

    let (scenario, mut history) = fixture(Some(1));
    history.push(history[0].clone());
    assert!(has_contract(&violations(&scenario, &history), "CONS-020"));
}

fn fixture(processed_record_count: Option<usize>) -> (Scenario, Vec<HistoryEntry>) {
    let scenario: Scenario = toml::from_str(include_str!(
        "../../../scenarios/kafka/classic-group-partial-checkpoint.toml"
    ))
    .unwrap_or_else(|error| panic!("partial checkpoint scenario: {error}"));
    let command_value = scenario
        .steps
        .iter()
        .find_map(|step| match &step.action {
            ScenarioAction::GroupReceive {
                consumer_id,
                method,
                receive_id,
                processing_acknowledgement_delay_ms,
                processed_record_count: Some(_),
                timeout_ms,
                ..
            } => Some(AdapterCommand::GroupReceive {
                consumer_id: consumer_id.clone(),
                method: *method,
                receive_id: receive_id.clone(),
                processing_acknowledgement_delay_ms: *processing_acknowledgement_delay_ms,
                processed_record_count,
                timeout_ms: *timeout_ms,
            }),
            _ => None,
        })
        .unwrap_or_else(|| panic!("partial group action missing"));
    (scenario, vec![command(0, command_value)])
}

fn violations(scenario: &Scenario, history: &[HistoryEntry]) -> Vec<testlab_schema::Violation> {
    let index = HistoryIndex::build(history);
    let mut violations = Vec::new();
    crate::group_partial_checkpoint::verify(scenario, &index, &mut violations);
    violations
}

fn has_contract(violations: &[testlab_schema::Violation], contract: &str) -> bool {
    violations
        .iter()
        .any(|violation| violation.contract_id.as_str() == contract)
}
