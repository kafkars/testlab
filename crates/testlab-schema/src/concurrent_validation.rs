//! Concurrent scenario validation owns actor identities, handles, and exact pairing.

use std::collections::BTreeSet;

use crate::scenario_action_validation::ActionStates;
use crate::{ConcurrencyId, ConcurrentActor, ScenarioAction};

const MIN_ACTORS: usize = 2;
const MAX_ACTORS: usize = 8;

pub(crate) fn validate(
    action: &ScenarioAction,
    state: &mut ActionStates,
    problems: &mut Vec<String>,
) {
    match action {
        ScenarioAction::StartConcurrentActors(action) => start(action, state, problems),
        ScenarioAction::JoinConcurrentActors(action) => {
            if !(100..=60_000).contains(&action.timeout_ms) {
                problems.push(format!(
                    "concurrent join {} timeout_ms must be between 100 and 60000",
                    action.concurrency_id
                ));
            }
            match state.active_concurrency.take() {
                Some(active) if active == action.concurrency_id => {}
                Some(active) => {
                    problems.push(format!(
                        "concurrent join {} does not match active group {active}",
                        action.concurrency_id
                    ));
                    state.active_concurrency = Some(active);
                }
                None => problems.push(format!(
                    "concurrent join {} has no active group",
                    action.concurrency_id
                )),
            }
        }
        _ => {}
    }
}

fn start(
    action: &crate::StartConcurrentActorsAction,
    state: &mut ActionStates,
    problems: &mut Vec<String>,
) {
    if !state.concurrency_ids.insert(action.concurrency_id.clone()) {
        problems.push(format!(
            "duplicate concurrent group id {}",
            action.concurrency_id
        ));
    }
    if !(MIN_ACTORS..=MAX_ACTORS).contains(&action.actors.len()) {
        problems.push(format!(
            "concurrent group {} must contain between {MIN_ACTORS} and {MAX_ACTORS} actors",
            action.concurrency_id
        ));
    }
    validate_actor_identities(action, state, problems);
    validate_sends(action, state, problems);
    validate_receives(action, state, problems);
    state.active_concurrency = Some(action.concurrency_id.clone());
}

fn validate_actor_identities(
    action: &crate::StartConcurrentActorsAction,
    state: &mut ActionStates,
    problems: &mut Vec<String>,
) {
    for actor in &action.actors {
        if !state.actor_ids.insert(actor.actor_id().clone()) {
            problems.push(format!(
                "duplicate concurrent actor id {}",
                actor.actor_id()
            ));
        }
    }
}

fn validate_sends(
    action: &crate::StartConcurrentActorsAction,
    state: &mut ActionStates,
    problems: &mut Vec<String>,
) {
    for actor in &action.actors {
        let ConcurrentActor::ProducerSend {
            producer_id,
            operation_id,
            record,
            ..
        } = actor
        else {
            continue;
        };
        crate::scenario_action_validation::require_open_producer(
            producer_id,
            &state.producers,
            problems,
        );
        crate::scenario_action_validation::validate_operation(
            operation_id,
            record,
            &mut state.operation_ids,
            &mut state.sends,
            problems,
        );
    }
}

fn validate_receives(
    action: &crate::StartConcurrentActorsAction,
    state: &mut ActionStates,
    problems: &mut Vec<String>,
) {
    let mut consumers = BTreeSet::new();
    for actor in &action.actors {
        let ConcurrentActor::AssignedReceive {
            consumer_id,
            receive_id,
            expected_operation_id,
            timeout_ms,
            ..
        } = actor
        else {
            continue;
        };
        crate::consumer_action_validation::receive(
            consumer_id,
            receive_id,
            *timeout_ms,
            &mut state.consumers,
            problems,
        );
        if state
            .consumers
            .get(consumer_id)
            .is_some_and(|consumer| consumer.group.is_some())
        {
            problems.push(format!(
                "concurrent receive actor {receive_id} requires a directly assigned consumer"
            ));
        }
        if !consumers.insert(consumer_id.clone()) {
            problems.push(format!(
                "concurrent group {} uses consumer {consumer_id} more than once",
                action.concurrency_id
            ));
        }
        if !state.operation_ids.insert(receive_id.clone()) {
            problems.push(format!("duplicate operation id {receive_id}"));
        }
        if !state.sends.contains(expected_operation_id) {
            problems.push(format!(
                "concurrent receive {receive_id} expects missing send {expected_operation_id}"
            ));
        }
    }
}

pub(crate) fn allowed_while_active(action: &ScenarioAction) -> bool {
    matches!(
        action,
        ScenarioAction::JoinConcurrentActors(_)
            | ScenarioAction::SetBrokerBehavior { .. }
            | ScenarioAction::ArmProtocolFault(_)
            | ScenarioAction::AlterNetworkFault(_)
            | ScenarioAction::CutNetworkConnections(_)
            | ScenarioAction::RestartBroker { .. }
            | ScenarioAction::StopBroker { .. }
            | ScenarioAction::StartBroker { .. }
            | ScenarioAction::StopBrokerRole { .. }
            | ScenarioAction::RestoreBrokerRole { .. }
            | ScenarioAction::AlterBrokerPolicy(_)
    )
}

pub(crate) fn active_id(state: &ActionStates) -> Option<&ConcurrencyId> {
    state.active_concurrency.as_ref()
}
