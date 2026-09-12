//! Direct-consumer event verification binds observer selection to exact public failure evidence.

use testlab_schema::{
    AdapterCommand, AdapterEvent, AssignedConsumerEventExpectation,
    AssignedConsumerEventObservationKind, AssignedConsumerFetchFenceObservation,
    AssignedConsumerPositionFenceObservation, Scenario, ScenarioAction, Violation,
};

use crate::index::HistoryIndex;
use crate::support::violation;

pub(crate) fn verify(scenario: &Scenario, index: &HistoryIndex, violations: &mut Vec<Violation>) {
    for step in &scenario.steps {
        let ScenarioAction::ObserveAssignedConsumerEvent(action) = &step.action else {
            continue;
        };
        let commands = index
            .commands
            .iter()
            .filter_map(|(sequence, _, command)| match command {
                AdapterCommand::ObserveAssignedConsumerEvent(command)
                    if command.operation_id == action.operation_id =>
                {
                    Some((*sequence, command))
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        let events = index
            .adapter_events
            .iter()
            .filter_map(|(sequence, envelope)| match &envelope.event {
                AdapterEvent::AssignedConsumerEventObserved(observation)
                    if observation.operation_id == action.operation_id =>
                {
                    Some((*sequence, observation))
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        let command_exact = matches!(commands.as_slice(), [(sequence, command)]
            if command.consumer_id == action.consumer_id
                && command.method == action.method
                && command.timeout_ms == action.timeout_ms
                && events.first().is_some_and(|(event_sequence, _)| sequence < event_sequence));
        let event_exact = matches!(events.as_slice(), [(_, observation)]
            if observation.consumer_id == action.consumer_id
                && observation.method == action.method
                && matches_expected(&action.expected, &observation.event));
        if command_exact && event_exact {
            continue;
        }
        violations.push(violation(
            "CONS-016",
            format!(
                "assigned-consumer event {} expected one ordered exact {:?} command and public event, observed {} command(s) and {} event(s)",
                action.operation_id,
                action.method,
                commands.len(),
                events.len()
            ),
            Some(action.operation_id.clone()),
            commands
                .iter()
                .map(|(sequence, _)| format!("history:{sequence}"))
                .chain(
                    events
                        .iter()
                        .map(|(sequence, _)| format!("history:{sequence}")),
                )
                .collect(),
        ));
    }
}

fn matches_expected(
    expected: &AssignedConsumerEventExpectation,
    actual: &AssignedConsumerEventObservationKind,
) -> bool {
    match (expected, actual) {
        (
            AssignedConsumerEventExpectation::PositionResolutionFailed {
                topic,
                partition,
                failure,
            },
            AssignedConsumerEventObservationKind::PositionResolutionFailed {
                fence,
                failure: actual_failure,
            },
        ) => failure == actual_failure && position_matches(topic, *partition, fence),
        (
            AssignedConsumerEventExpectation::FetchThrottleFailed {
                topic,
                partition,
                failure,
            },
            AssignedConsumerEventObservationKind::FetchThrottleFailed {
                fence,
                failure: actual_failure,
            },
        ) => failure == actual_failure && fetch_matches(topic, *partition, fence),
        (
            AssignedConsumerEventExpectation::FetchFailed {
                topic,
                partition,
                failure,
            },
            AssignedConsumerEventObservationKind::FetchFailed {
                fence,
                failure: actual_failure,
            },
        ) => failure == actual_failure && fetch_matches(topic, *partition, fence),
        _ => false,
    }
}

fn position_matches(
    topic: &str,
    partition: i32,
    fence: &AssignedConsumerPositionFenceObservation,
) -> bool {
    fence.topic == topic
        && fence.partition == partition
        && fence.assignment_epoch > 0
        && fence.position_epoch > 0
}

fn fetch_matches(
    topic: &str,
    partition: i32,
    fence: &AssignedConsumerFetchFenceObservation,
) -> bool {
    position_matches(topic, partition, &fence.position) && fence.fetch_revision > 0
}
