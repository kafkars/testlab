//! Immediate group-receive tests reject observer substitution.

use testlab_schema::{
    AdapterCommand, GroupConsumerReceiveMethod, HistoryEntry, Scenario, ScenarioAction,
};

use crate::index::HistoryIndex;
use crate::verify_fixture::command;

#[test]
fn exact_immediate_group_command_passes() {
    let (scenario, history) = fixture(GroupConsumerReceiveMethod::TryTakeBatch);
    assert!(violations(&scenario, &history).is_empty());
}

#[test]
fn waiting_or_duplicate_group_command_fails() {
    let (scenario, history) = fixture(GroupConsumerReceiveMethod::Recv);
    assert!(has_contract(&violations(&scenario, &history), "CONS-018"));

    let (scenario, mut history) = fixture(GroupConsumerReceiveMethod::TryTakeBatch);
    history.push(history[0].clone());
    assert!(has_contract(&violations(&scenario, &history), "CONS-018"));
}

fn fixture(method: GroupConsumerReceiveMethod) -> (Scenario, Vec<HistoryEntry>) {
    let scenario: Scenario = toml::from_str(include_str!(
        "../../../scenarios/kafka/classic-group-immediate-batch.toml"
    ))
    .unwrap_or_else(|error| panic!("immediate group scenario: {error}"));
    let command_value = scenario
        .steps
        .iter()
        .find_map(|step| match &step.action {
            ScenarioAction::GroupReceive {
                consumer_id,
                method: GroupConsumerReceiveMethod::TryTakeBatch,
                checkpoint_method,
                receive_id,
                processing_acknowledgement_delay_ms,
                processed_record_count,
                timeout_ms,
                ..
            } => Some(AdapterCommand::GroupReceive {
                consumer_id: consumer_id.clone(),
                method,
                checkpoint_method: *checkpoint_method,
                receive_id: receive_id.clone(),
                processing_acknowledgement_delay_ms: *processing_acknowledgement_delay_ms,
                processed_record_count: *processed_record_count,
                timeout_ms: *timeout_ms,
            }),
            _ => None,
        })
        .unwrap_or_else(|| panic!("immediate group action missing"));
    (scenario, vec![command(0, command_value)])
}

fn violations(scenario: &Scenario, history: &[HistoryEntry]) -> Vec<testlab_schema::Violation> {
    let index = HistoryIndex::build(history);
    let mut violations = Vec::new();
    crate::group_consumer_receive_method::verify(scenario, &index, &mut violations);
    violations
}

fn has_contract(violations: &[testlab_schema::Violation], contract: &str) -> bool {
    violations
        .iter()
        .any(|violation| violation.contract_id.as_str() == contract)
}
