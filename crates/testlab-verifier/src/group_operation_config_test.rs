//! Aggregate operation-policy verification rejects setter substitution and duplication.

use testlab_schema::{
    AdapterCommand, GroupOperationConfigMethod, HistoryEntry, Scenario, ScenarioAction,
};

use crate::index::HistoryIndex;
use crate::verify_fixture::command;

#[test]
fn exact_aggregate_group_creation_passes() {
    let (scenario, history) = fixture(GroupOperationConfigMethod::OperationConfig);
    assert!(violations(&scenario, &history).is_empty());
}

#[test]
fn individual_or_duplicate_group_creation_fails() {
    let (scenario, history) = fixture(GroupOperationConfigMethod::IndividualSetters);
    assert!(has_contract(&violations(&scenario, &history), "CONS-024"));

    let (scenario, mut history) = fixture(GroupOperationConfigMethod::OperationConfig);
    history.push(history[0].clone());
    assert!(has_contract(&violations(&scenario, &history), "CONS-024"));
}

fn fixture(method: GroupOperationConfigMethod) -> (Scenario, Vec<HistoryEntry>) {
    let scenario: Scenario = toml::from_str(include_str!(
        "../../../scenarios/kafka/classic-group-seek-replay.toml"
    ))
    .unwrap_or_else(|error| panic!("aggregate group scenario: {error}"));
    let command_value = scenario
        .steps
        .iter()
        .find_map(|step| match &step.action {
            ScenarioAction::CreateGroupConsumer {
                client_id,
                consumer_id,
                group_id,
                topics,
                protocol,
                configuration: Some(configuration),
            } if configuration.operation_config_method
                == GroupOperationConfigMethod::OperationConfig =>
            {
                let mut configuration = configuration.clone();
                configuration.operation_config_method = method;
                Some(AdapterCommand::CreateGroupConsumer {
                    client_id: client_id.clone(),
                    consumer_id: consumer_id.clone(),
                    group_id: group_id.clone(),
                    topics: topics.clone(),
                    protocol: *protocol,
                    configuration: Some(configuration),
                })
            }
            _ => None,
        })
        .unwrap_or_else(|| panic!("aggregate group creation missing"));
    (scenario, vec![command(0, command_value)])
}

fn violations(scenario: &Scenario, history: &[HistoryEntry]) -> Vec<testlab_schema::Violation> {
    let index = HistoryIndex::build(history);
    let mut violations = Vec::new();
    crate::group_operation_config::verify(scenario, &index, &mut violations);
    violations
}

fn has_contract(violations: &[testlab_schema::Violation], contract: &str) -> bool {
    violations
        .iter()
        .any(|violation| violation.contract_id.as_str() == contract)
}
