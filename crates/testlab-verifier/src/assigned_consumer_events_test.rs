//! Direct-consumer event tests reject observer, failure, and cardinality substitution.

use testlab_schema::{
    AdapterCommand, AdapterEvent, AssignedConsumerEventExpectation, AssignedConsumerEventMethod,
    AssignedConsumerEventObservation, AssignedConsumerEventObservationKind,
    AssignedConsumerFetchFenceObservation, AssignedConsumerPositionFailure,
    AssignedConsumerPositionFenceObservation, HistoryEntry, HistoryPayload,
    ObserveAssignedConsumerEventCommand, Scenario, ScenarioAction,
};

use crate::index::HistoryIndex;
use crate::verify_fixture::{command, event};

#[test]
fn exact_waiting_and_immediate_events_pass() {
    let scenario = scenario();
    assert!(violations(&scenario, &history(&scenario)).is_empty());
}

#[test]
fn observer_substitution_fails() {
    let scenario = scenario();
    let mut history = history(&scenario);
    let HistoryPayload::HarnessCommand { command } = &mut history[0].payload else {
        panic!("first event command missing");
    };
    let AdapterCommand::ObserveAssignedConsumerEvent(command) = &mut command.command else {
        panic!("assigned event command missing");
    };
    command.method = AssignedConsumerEventMethod::TryTakeEvent;
    assert!(has_contract(&violations(&scenario, &history), "CONS-016"));
}

#[test]
fn broker_code_or_duplicate_event_fails() {
    let scenario = scenario();
    let mut history = history(&scenario);
    let HistoryPayload::AdapterEvent { event } = &mut history[1].payload else {
        panic!("first event observation missing");
    };
    let AdapterEvent::AssignedConsumerEventObserved(observation) = &mut event.event else {
        panic!("assigned event observation missing");
    };
    let AssignedConsumerEventObservationKind::PositionResolutionFailed { failure, .. } =
        &mut observation.event
    else {
        panic!("position failure missing");
    };
    *failure = AssignedConsumerPositionFailure::Broker { code: 30 };
    assert!(has_contract(&violations(&scenario, &history), "CONS-016"));

    let mut history = history(&scenario);
    history.push(history[1].clone());
    assert!(has_contract(&violations(&scenario, &history), "CONS-016"));
}

fn scenario() -> Scenario {
    toml::from_str(include_str!(
        "../../../scenarios/kafka/assigned-consumer-event-authorization-recovery.toml"
    ))
    .unwrap_or_else(|error| panic!("assigned event scenario: {error}"))
}

fn history(scenario: &Scenario) -> Vec<HistoryEntry> {
    let mut history = Vec::new();
    for action in scenario.steps.iter().filter_map(|step| match &step.action {
        ScenarioAction::ObserveAssignedConsumerEvent(action) => Some(action),
        _ => None,
    }) {
        let sequence = history.len() as u64;
        history.push(command(
            sequence,
            AdapterCommand::ObserveAssignedConsumerEvent(ObserveAssignedConsumerEventCommand {
                operation_id: action.operation_id.clone(),
                consumer_id: action.consumer_id.clone(),
                method: action.method,
                timeout_ms: action.timeout_ms,
            }),
        ));
        history.push(event(
            sequence + 1,
            AdapterEvent::AssignedConsumerEventObserved(AssignedConsumerEventObservation {
                operation_id: action.operation_id.clone(),
                consumer_id: action.consumer_id.clone(),
                method: action.method,
                event: observed(&action.expected),
            }),
        ));
    }
    history
}

fn observed(expected: &AssignedConsumerEventExpectation) -> AssignedConsumerEventObservationKind {
    match expected {
        AssignedConsumerEventExpectation::PositionResolutionFailed {
            topic,
            partition,
            failure,
        } => AssignedConsumerEventObservationKind::PositionResolutionFailed {
            fence: position_fence(topic, *partition),
            failure: *failure,
        },
        AssignedConsumerEventExpectation::FetchThrottleFailed {
            topic,
            partition,
            failure,
        } => AssignedConsumerEventObservationKind::FetchThrottleFailed {
            fence: fetch_fence(topic, *partition),
            failure: *failure,
        },
        AssignedConsumerEventExpectation::FetchFailed {
            topic,
            partition,
            failure,
        } => AssignedConsumerEventObservationKind::FetchFailed {
            fence: fetch_fence(topic, *partition),
            failure: *failure,
        },
    }
}

fn position_fence(topic: &str, partition: i32) -> AssignedConsumerPositionFenceObservation {
    AssignedConsumerPositionFenceObservation {
        topic: topic.to_owned(),
        partition,
        assignment_epoch: 1,
        position_epoch: 1,
    }
}

fn fetch_fence(topic: &str, partition: i32) -> AssignedConsumerFetchFenceObservation {
    AssignedConsumerFetchFenceObservation {
        position: position_fence(topic, partition),
        fetch_revision: 1,
    }
}

fn violations(scenario: &Scenario, history: &[HistoryEntry]) -> Vec<testlab_schema::Violation> {
    let index = HistoryIndex::build(history);
    let mut violations = Vec::new();
    crate::assigned_consumer_events::verify(scenario, &index, &mut violations);
    violations
}

fn has_contract(violations: &[testlab_schema::Violation], contract: &str) -> bool {
    violations
        .iter()
        .any(|violation| violation.contract_id.as_str() == contract)
}
