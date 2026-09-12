//! Operation-policy verification rejects individual and aggregate substitution.

use testlab_schema::{
    AdapterCommand, GroupOperationConfigMethod, HistoryEntry, Scenario, ScenarioAction,
};

use crate::index::HistoryIndex;
use crate::verify_fixture::command;

#[test]
fn exact_group_operation_methods_pass() {
    for method in [
        GroupOperationConfigMethod::IndividualSetters,
        GroupOperationConfigMethod::OperationConfig,
    ] {
        let (scenario, history) = fixture(method, method);
        assert!(violations(&scenario, &history).is_empty());
    }
}

#[test]
fn substituted_or_duplicate_group_creation_fails() {
    let (scenario, history) = fixture(
        GroupOperationConfigMethod::OperationConfig,
        GroupOperationConfigMethod::IndividualSetters,
    );
    assert!(has_contract(&violations(&scenario, &history), "CONS-024"));

    let (scenario, history) = fixture(
        GroupOperationConfigMethod::IndividualSetters,
        GroupOperationConfigMethod::OperationConfig,
    );
    assert!(has_contract(&violations(&scenario, &history), "CONS-024"));

    let (scenario, mut history) = fixture(
        GroupOperationConfigMethod::OperationConfig,
        GroupOperationConfigMethod::OperationConfig,
    );
    history.push(history[0].clone());
    assert!(has_contract(&violations(&scenario, &history), "CONS-024"));
}

fn fixture(
    selected: GroupOperationConfigMethod,
    observed: GroupOperationConfigMethod,
) -> (Scenario, Vec<HistoryEntry>) {
    let source = match selected {
        GroupOperationConfigMethod::IndividualSetters => {
            include_str!("../../../scenarios/kafka/consumer-protocol-group-seek-replay.toml")
        }
        GroupOperationConfigMethod::OperationConfig => {
            include_str!("../../../scenarios/kafka/classic-group-seek-replay.toml")
        }
    };
    let scenario: Scenario =
        toml::from_str(source).unwrap_or_else(|error| panic!("group operation scenario: {error}"));
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
            } if configuration.operation_config_method == selected => {
                let mut configuration = configuration.clone();
                configuration.operation_config_method = observed;
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
        .unwrap_or_else(|| panic!("selected group operation creation missing"));
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
