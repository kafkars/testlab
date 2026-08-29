//! Concurrent verification binds external scheduling to public outcomes and broker truth.

use testlab_schema::{
    BrokerObservation, ConcurrentActor, Scenario, ScenarioAction, Violation, VisibilityExpectation,
};

use crate::concurrent_support::{boundary_references, command_actor, concurrent_references};
use crate::index::{ConcurrentPublicEventKind, HistoryIndex};
use crate::support::violation;

pub(crate) fn verify(
    scenario: &Scenario,
    index: &HistoryIndex,
    observations: &[BrokerObservation],
    violations: &mut Vec<Violation>,
) {
    for step in &scenario.steps {
        let ScenarioAction::StartConcurrentActors(action) = &step.action else {
            continue;
        };
        verify_boundaries(action, index, violations);
        verify_membership(action, index, violations);
        verify_outcomes(action, scenario, index, violations);
        verify_truth(action, scenario, index, observations, violations);
    }
}

fn verify_boundaries(
    action: &testlab_schema::StartConcurrentActorsAction,
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) {
    let starts = index.concurrent_starts.get(&action.concurrency_id);
    let joins = index.concurrent_joins.get(&action.concurrency_id);
    let started = index.concurrent_started.get(&action.concurrency_id);
    let completed = index.concurrent_completed.get(&action.concurrency_id);
    let exact = starts.is_some_and(|values| values.len() == 1)
        && joins.is_some_and(|values| values.len() == 1)
        && started.is_some_and(|values| values.len() == 1)
        && completed.is_some_and(|values| values.len() == 1);
    let ordered = exact
        && starts.is_some_and(|starts| {
            joins.is_some_and(|joins| {
                started.is_some_and(|started| {
                    completed.is_some_and(|completed| {
                        starts[0].sequence < started[0].sequence
                            && started[0].sequence < joins[0].sequence
                            && joins[0].sequence < completed[0].sequence
                            && starts[0].command_id == started[0].command_id
                            && joins[0].command_id == completed[0].command_id
                    })
                })
            })
        });
    if !ordered {
        violations.push(violation(
            "CONCUR-001",
            format!(
                "concurrent group {} requires one exactly correlated ordered start and join boundary",
                action.concurrency_id
            ),
            None,
            boundary_references(starts, joins, started, completed),
        ));
    }
}

fn verify_membership(
    action: &testlab_schema::StartConcurrentActorsAction,
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) {
    let expected_actors = action.actors.iter().map(command_actor).collect::<Vec<_>>();
    let expected_ids = action
        .actors
        .iter()
        .map(|actor| actor.actor_id().clone())
        .collect::<Vec<_>>();
    let starts = index.concurrent_starts.get(&action.concurrency_id);
    let started = index.concurrent_started.get(&action.concurrency_id);
    let completed = index.concurrent_completed.get(&action.concurrency_id);
    let actor_completions = index
        .concurrent_actor_completions
        .get(&action.concurrency_id);
    let expected_completions = action
        .actors
        .iter()
        .map(|actor| (actor.actor_id(), actor.operation_id()))
        .collect::<Vec<_>>();
    let exact = starts
        .is_some_and(|values| values.len() == 1 && values[0].actors == expected_actors)
        && started.is_some_and(|values| values.len() == 1 && values[0].actor_ids == expected_ids)
        && completed.is_some_and(|values| values.len() == 1 && values[0].actor_ids == expected_ids)
        && actor_completions.is_some_and(|values| {
            values.len() == expected_completions.len()
                && values
                    .iter()
                    .zip(&expected_completions)
                    .all(|(actual, (actor, operation))| {
                        &actual.actor_id == *actor && &actual.operation_id == *operation
                    })
        });
    if !exact {
        violations.push(violation(
            "CONCUR-002",
            format!(
                "concurrent group {} did not preserve its exact ordered actor membership",
                action.concurrency_id
            ),
            None,
            concurrent_references(index, &action.concurrency_id),
        ));
    }
}

fn verify_outcomes(
    action: &testlab_schema::StartConcurrentActorsAction,
    scenario: &Scenario,
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) {
    let Some(join) = index
        .concurrent_joins
        .get(&action.concurrency_id)
        .and_then(|values| (values.len() == 1).then_some(&values[0]))
    else {
        return;
    };
    let Some(group_completed) = index
        .concurrent_completed
        .get(&action.concurrency_id)
        .and_then(|values| (values.len() == 1).then_some(&values[0]))
    else {
        return;
    };
    for actor in &action.actors {
        let completion = index
            .concurrent_actor_completions
            .get(&action.concurrency_id)
            .into_iter()
            .flatten()
            .filter(|value| value.actor_id == *actor.actor_id())
            .collect::<Vec<_>>();
        let public = index
            .concurrent_public_events
            .iter()
            .filter(|value| value.operation_id == *actor.operation_id())
            .collect::<Vec<_>>();
        let valid = completion.len() == 1
            && completion[0].command_id == join.command_id
            && join.sequence < completion[0].sequence
            && completion[0].sequence < group_completed.sequence
            && public.iter().all(|value| {
                value.command_id == join.command_id
                    && join.sequence < value.sequence
                    && value.sequence < completion[0].sequence
            })
            && expected_public_shape(actor, scenario, &public);
        if !valid {
            violations.push(violation(
                "CONCUR-003",
                format!(
                    "concurrent actor {} did not expose one complete public outcome inside group {}",
                    actor.actor_id(), action.concurrency_id
                ),
                Some(actor.operation_id().clone()),
                public
                    .iter()
                    .map(|value| format!("history:{}", value.sequence))
                    .chain(completion.iter().map(|value| format!("history:{}", value.sequence)))
                    .collect(),
            ));
        }
    }
}

fn expected_public_shape(
    actor: &ConcurrentActor,
    scenario: &Scenario,
    public: &[&crate::index::IndexedConcurrentPublicEvent],
) -> bool {
    match actor {
        ConcurrentActor::ProducerSend { operation_id, .. } => {
            let accepted = scenario
                .assertions
                .iter()
                .find(|assertion| &assertion.operation_id == operation_id)
                .is_some_and(|assertion| assertion.accepted);
            let kinds = public.iter().map(|value| value.kind).collect::<Vec<_>>();
            if accepted {
                kinds
                    == [
                        ConcurrentPublicEventKind::Accepted,
                        ConcurrentPublicEventKind::Terminal,
                    ]
            } else {
                kinds == [ConcurrentPublicEventKind::Rejected]
            }
        }
        ConcurrentActor::AssignedReceive { .. } => {
            public.len() == 1 && public[0].kind == ConcurrentPublicEventKind::Receive
        }
    }
}

fn verify_truth(
    action: &testlab_schema::StartConcurrentActorsAction,
    scenario: &Scenario,
    index: &HistoryIndex,
    observations: &[BrokerObservation],
    violations: &mut Vec<Violation>,
) {
    for actor in &action.actors {
        let exact = match actor {
            ConcurrentActor::ProducerSend { operation_id, .. } => {
                let count = observations
                    .iter()
                    .filter(|value| &value.operation_id == operation_id)
                    .count();
                scenario
                    .assertions
                    .iter()
                    .find(|assertion| &assertion.operation_id == operation_id)
                    .is_some_and(|assertion| match assertion.visibility {
                        VisibilityExpectation::Absent => count == 0,
                        VisibilityExpectation::ExactlyOnce => count == 1,
                        VisibilityExpectation::ZeroOrOne => count <= 1,
                    })
            }
            ConcurrentActor::AssignedReceive {
                receive_id,
                expected_operation_id,
                ..
            } => index.receives.get(receive_id).is_some_and(|values| {
                values.len() == 1
                    && crate::consumer::sent_record(scenario, expected_operation_id).is_some_and(
                        |record| {
                            values[0].records.len() == 1
                                && crate::consumer::exact_record(&values[0].records[0], record)
                        },
                    )
            }),
        };
        if !exact {
            violations.push(violation(
                "CONCUR-004",
                format!(
                    "concurrent actor {} lacks its exact independent broker or public record truth",
                    actor.actor_id()
                ),
                Some(actor.operation_id().clone()),
                Vec::new(),
            ));
        }
    }
}
