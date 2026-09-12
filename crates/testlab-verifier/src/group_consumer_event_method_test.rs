//! Group-transition observer tests reject command or evidence substitution.

use testlab_schema::{
    AdapterCommand, AdapterEvent, GroupAssignmentsObservation, GroupConsumerEventMethod,
    HistoryEntry, HistoryPayload, ObserveGroupAssignmentsCommand, Scenario, ScenarioAction,
};

use crate::index::HistoryIndex;
use crate::verify_fixture::{command, event};

#[test]
fn exact_waiting_and_immediate_observers_pass() {
    let scenario = scenario();
    assert!(violations(&scenario, &history(&scenario)).is_empty());
}

#[test]
fn observer_substitution_or_duplicate_event_fails() {
    let scenario = scenario();
    let mut history = history(&scenario);
    let HistoryPayload::HarnessCommand { command } = &mut history[0].payload else {
        panic!("first assignment command missing");
    };
    let AdapterCommand::ObserveGroupAssignments(command) = &mut command.command else {
        panic!("assignment observation command missing");
    };
    command.method = GroupConsumerEventMethod::TryTakeEvent;
    assert!(has_contract(&violations(&scenario, &history), "CONS-026"));

    let mut history = history(&scenario);
    let HistoryPayload::AdapterEvent { event } = &mut history[1].payload else {
        panic!("first assignment event missing");
    };
    let AdapterEvent::GroupAssignmentsObserved(observation) = &mut event.event else {
        panic!("assignment observation event missing");
    };
    observation.method = GroupConsumerEventMethod::TryTakeEvent;
    assert!(has_contract(&violations(&scenario, &history), "CONS-026"));

    let mut history = history(&scenario);
    history.push(history[1].clone());
    assert!(has_contract(&violations(&scenario, &history), "CONS-026"));
}

fn scenario() -> Scenario {
    toml::from_str(include_str!(
        "../../../scenarios/kafka/classic-group-membership-ownership.toml"
    ))
    .unwrap_or_else(|error| panic!("group ownership scenario: {error}"))
}

fn history(scenario: &Scenario) -> Vec<HistoryEntry> {
    let mut history = Vec::new();
    for action in scenario.steps.iter().filter_map(|step| match &step.action {
        ScenarioAction::ObserveGroupAssignments(action) => Some(action),
        _ => None,
    }) {
        let sequence = history.len() as u64;
        history.push(command(
            sequence,
            AdapterCommand::ObserveGroupAssignments(ObserveGroupAssignmentsCommand {
                operation_id: action.operation_id.clone(),
                consumer_ids: action.consumer_ids.clone(),
                method: action.method,
                timeout_ms: action.timeout_ms,
            }),
        ));
        history.push(event(
            sequence + 1,
            AdapterEvent::GroupAssignmentsObserved(GroupAssignmentsObservation {
                operation_id: action.operation_id.clone(),
                method: action.method,
                transitions: Vec::new(),
                assignments: Vec::new(),
            }),
        ));
    }
    history
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
