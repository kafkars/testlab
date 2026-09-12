//! Full group-checkpoint tests reject compatibility-method substitution.

use testlab_schema::{
    AdapterCommand, GroupCheckpointMethod, HistoryEntry, Scenario, ScenarioAction,
};

use crate::index::HistoryIndex;
use crate::verify_fixture::command;

#[test]
fn exact_into_checkpoint_command_passes() {
    let (scenario, history) = fixture(GroupCheckpointMethod::IntoCheckpoint);
    assert!(violations(&scenario, &history).is_empty());
}

#[test]
fn canonical_or_duplicate_checkpoint_command_fails() {
    let (scenario, history) = fixture(GroupCheckpointMethod::Checkpoint);
    assert!(has_contract(&violations(&scenario, &history), "CONS-025"));

    let (scenario, mut history) = fixture(GroupCheckpointMethod::IntoCheckpoint);
    history.push(history[0].clone());
    assert!(has_contract(&violations(&scenario, &history), "CONS-025"));
}

fn fixture(method: GroupCheckpointMethod) -> (Scenario, Vec<HistoryEntry>) {
    let scenario: Scenario = toml::from_str(include_str!(
        "../../../scenarios/kafka/classic-group-round-trip.toml"
    ))
    .unwrap_or_else(|error| panic!("classic group scenario: {error}"));
    let command_value = scenario
        .steps
        .iter()
        .find_map(|step| match &step.action {
            ScenarioAction::GroupReceive {
                checkpoint_method: GroupCheckpointMethod::IntoCheckpoint,
                ..
            } => {
                let mut command = super::command(&step.action);
                if let AdapterCommand::GroupReceive {
                    checkpoint_method, ..
                } = &mut command
                {
                    *checkpoint_method = method;
                }
                Some(command)
            }
            _ => None,
        })
        .unwrap_or_else(|| panic!("into_checkpoint group action missing"));
    (scenario, vec![command(0, command_value)])
}

fn violations(scenario: &Scenario, history: &[HistoryEntry]) -> Vec<testlab_schema::Violation> {
    let index = HistoryIndex::build(history);
    let mut violations = Vec::new();
    super::verify(scenario, &index, &mut violations);
    violations
}

fn has_contract(violations: &[testlab_schema::Violation], contract: &str) -> bool {
    violations
        .iter()
        .any(|violation| violation.contract_id.as_str() == contract)
}
