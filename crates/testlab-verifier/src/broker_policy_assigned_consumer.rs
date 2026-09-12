//! Direct-consumer topic-READ policy proof joins retained failures to restored records.

use testlab_schema::{
    AdapterCommand, AdapterEvent, AssignedConsumerEventExpectation,
    AssignedConsumerEventObservationKind, AssignedConsumerPositionFailure, BrokerObservation,
    Scenario, ScenarioAction,
};

use crate::broker_policy::{PolicyWindow, active};
use crate::index::HistoryIndex;

pub(crate) fn denial(
    scenario: &Scenario,
    topic: &str,
    window: &PolicyWindow<'_>,
    index: &HistoryIndex,
) -> Option<u64> {
    scenario.steps.iter().find_map(|step| {
        let ScenarioAction::ObserveAssignedConsumerEvent(action) = &step.action else {
            return None;
        };
        let AssignedConsumerEventExpectation::PositionResolutionFailed {
            topic: expected_topic,
            partition,
            failure: AssignedConsumerPositionFailure::Broker { code: 29 },
        } = &action.expected
        else {
            return None;
        };
        if expected_topic != topic {
            return None;
        }
        let commands = index
            .commands
            .iter()
            .filter_map(|(sequence, _, command)| match command {
                AdapterCommand::ObserveAssignedConsumerEvent(command)
                    if command.operation_id == action.operation_id
                        && command.consumer_id == action.consumer_id
                        && command.method == action.method
                        && command.timeout_ms == action.timeout_ms =>
                {
                    Some(*sequence)
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        let events = index
            .adapter_events
            .iter()
            .filter_map(|(sequence, envelope)| match &envelope.event {
                AdapterEvent::AssignedConsumerEventObserved(observation)
                    if observation.operation_id == action.operation_id
                        && observation.consumer_id == action.consumer_id
                        && observation.method == action.method
                        && event_matches(&observation.event, topic, *partition) =>
                {
                    Some(*sequence)
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        match (commands.as_slice(), events.as_slice()) {
            ([command], [event])
                if command < event && active(window, *command) && active(window, *event) =>
            {
                Some(*event)
            }
            _ => None,
        }
    })
}

pub(crate) fn recovery(
    scenario: &Scenario,
    topic: &str,
    window: &PolicyWindow<'_>,
    index: &HistoryIndex,
    observations: &[BrokerObservation],
) -> Option<u64> {
    scenario.steps.iter().find_map(|step| {
        let ScenarioAction::Receive {
            consumer_id,
            method,
            receive_id,
            expected_operation_id,
            timeout_ms,
        } = &step.action
        else {
            return None;
        };
        if send_topic(scenario, expected_operation_id)? != topic {
            return None;
        }
        let commands = index
            .commands
            .iter()
            .filter_map(|(sequence, _, command)| match command {
                AdapterCommand::Receive {
                    consumer_id: actual_consumer,
                    method: actual_method,
                    receive_id: actual_receive,
                    timeout_ms: actual_timeout,
                } if actual_consumer == consumer_id
                    && actual_method == method
                    && actual_receive == receive_id
                    && actual_timeout == timeout_ms =>
                {
                    Some(*sequence)
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        let [command] = commands.as_slice() else {
            return None;
        };
        let [receive] = index.receives.get(receive_id)?.as_slice() else {
            return None;
        };
        (*command > window.absent.observation_sequence
            && receive.history_sequence > window.absent.observation_sequence
            && receive.records.len() == 1
            && receive.records[0].topic == topic
            && observations
                .iter()
                .filter(|observation| observation.operation_id == *expected_operation_id)
                .count()
                == 1)
            .then_some(receive.history_sequence)
    })
}

fn event_matches(
    event: &AssignedConsumerEventObservationKind,
    topic: &str,
    partition: i32,
) -> bool {
    matches!(event,
        AssignedConsumerEventObservationKind::PositionResolutionFailed { fence, failure }
            if fence.topic == topic
                && fence.partition == partition
                && fence.assignment_epoch > 0
                && fence.position_epoch > 0
                && *failure == AssignedConsumerPositionFailure::Broker { code: 29 })
}

fn send_topic<'a>(
    scenario: &'a Scenario,
    operation_id: &testlab_schema::OperationId,
) -> Option<&'a str> {
    scenario.steps.iter().find_map(|step| match &step.action {
        ScenarioAction::Send {
            operation_id: actual,
            record,
            ..
        } if actual == operation_id => Some(record.topic.as_str()),
        _ => None,
    })
}
