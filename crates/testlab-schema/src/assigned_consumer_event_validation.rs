//! Direct-consumer event validation owns observer identity, bounds, and expected failure shape.

use crate::{
    AssignedConsumerEventExpectation, AssignedConsumerFetchFailure,
    AssignedConsumerPositionFailure, ObserveAssignedConsumerEventAction,
};

pub(crate) fn validate(
    action: &ObserveAssignedConsumerEventAction,
    state: &mut crate::scenario_action_state::ActionStates,
    problems: &mut Vec<String>,
) {
    if let Some(consumer) =
        crate::consumer_action_validation::open(&action.consumer_id, &mut state.consumers, problems)
    {
        if !consumer.assigned {
            problems.push(format!(
                "consumer {} observed an event before assignment",
                action.consumer_id
            ));
        }
        if consumer.group.is_some() {
            problems.push(format!(
                "consumer {} is not a directly assigned consumer",
                action.consumer_id
            ));
        }
    }
    if !state.operation_ids.insert(action.operation_id.clone()) {
        problems.push(format!("duplicate operation id {}", action.operation_id));
    }
    if !(100..=60_000).contains(&action.timeout_ms) {
        problems.push(format!(
            "assigned-consumer event {} timeout_ms must be between 100 and 60000",
            action.operation_id
        ));
    }
    let (topic, partition) = expected_target(&action.expected);
    if topic.is_empty() || topic.len() > 249 {
        problems.push(format!(
            "assigned-consumer event {} has invalid topic",
            action.operation_id
        ));
    }
    if partition < 0 {
        problems.push(format!(
            "assigned-consumer event {} has negative partition {partition}",
            action.operation_id
        ));
    }
    validate_broker_code(&action.expected, &action.operation_id, problems);
}

fn expected_target(expected: &AssignedConsumerEventExpectation) -> (&str, i32) {
    match expected {
        AssignedConsumerEventExpectation::PositionResolutionFailed {
            topic, partition, ..
        }
        | AssignedConsumerEventExpectation::FetchThrottleFailed {
            topic, partition, ..
        }
        | AssignedConsumerEventExpectation::FetchFailed {
            topic, partition, ..
        } => (topic, *partition),
    }
}

fn validate_broker_code(
    expected: &AssignedConsumerEventExpectation,
    operation_id: &crate::OperationId,
    problems: &mut Vec<String>,
) {
    let broker_code = match expected {
        AssignedConsumerEventExpectation::PositionResolutionFailed {
            failure: AssignedConsumerPositionFailure::Broker { code },
            ..
        }
        | AssignedConsumerEventExpectation::FetchFailed {
            failure: AssignedConsumerFetchFailure::Broker { code },
            ..
        } => Some(*code),
        _ => None,
    };
    if broker_code == Some(0) {
        problems.push(format!(
            "assigned-consumer event {operation_id} has zero broker error code"
        ));
    }
}
