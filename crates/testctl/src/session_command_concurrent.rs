//! Concurrent action translation strips scenario expectations at the adapter boundary.

use std::collections::BTreeSet;

use testlab_schema::{
    AdapterCommand, ConcurrentActor, ConcurrentActorCommand, Scenario, ScenarioAction,
    StartConcurrentActorsCommand,
};

use crate::runner_protocol::ExpectedEvent;
use crate::runner_protocol_concurrent::ConcurrentExpectation;

pub(crate) fn translate(
    action: &ScenarioAction,
    scenario: &Scenario,
) -> Option<(AdapterCommand, ExpectedEvent)> {
    match action {
        ScenarioAction::StartConcurrentActors(action) => {
            let expectation = expectation(action);
            Some((
                AdapterCommand::StartConcurrentActors(StartConcurrentActorsCommand {
                    concurrency_id: action.concurrency_id.clone(),
                    actors: action.actors.iter().map(command_actor).collect(),
                }),
                ExpectedEvent::ConcurrentActorsStarted(expectation),
            ))
        }
        ScenarioAction::JoinConcurrentActors(action) => {
            let started = scenario.steps.iter().find_map(|step| match &step.action {
                ScenarioAction::StartConcurrentActors(started)
                    if started.concurrency_id == action.concurrency_id =>
                {
                    Some(started)
                }
                _ => None,
            })?;
            Some((
                AdapterCommand::JoinConcurrentActors {
                    concurrency_id: action.concurrency_id.clone(),
                    timeout_ms: action.timeout_ms,
                },
                ExpectedEvent::ConcurrentActorsCompleted(expectation(started)),
            ))
        }
        _ => None,
    }
}

fn command_actor(actor: &ConcurrentActor) -> ConcurrentActorCommand {
    match actor {
        ConcurrentActor::ProducerSend {
            actor_id,
            producer_id,
            operation_id,
            record,
        } => ConcurrentActorCommand::ProducerSend {
            actor_id: actor_id.clone(),
            producer_id: producer_id.clone(),
            operation_id: operation_id.clone(),
            record: record.clone(),
        },
        ConcurrentActor::AssignedReceive {
            actor_id,
            consumer_id,
            receive_id,
            timeout_ms,
            ..
        } => ConcurrentActorCommand::AssignedReceive {
            actor_id: actor_id.clone(),
            consumer_id: consumer_id.clone(),
            receive_id: receive_id.clone(),
            timeout_ms: *timeout_ms,
        },
    }
}

fn expectation(action: &testlab_schema::StartConcurrentActorsAction) -> ConcurrentExpectation {
    let mut sends = BTreeSet::new();
    let mut receives = BTreeSet::new();
    for actor in &action.actors {
        match actor {
            ConcurrentActor::ProducerSend { operation_id, .. } => {
                sends.insert(operation_id.clone());
            }
            ConcurrentActor::AssignedReceive { receive_id, .. } => {
                receives.insert(receive_id.clone());
            }
        }
    }
    ConcurrentExpectation {
        concurrency_id: action.concurrency_id.clone(),
        actors: action
            .actors
            .iter()
            .map(|actor| (actor.actor_id().clone(), actor.operation_id().clone()))
            .collect(),
        sends,
        receives,
    }
}
