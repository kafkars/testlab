//! Concurrent schema tests pin exact pairing, actor identity, and resource ownership.

use crate::{ConcurrencyId, ConcurrentActor, Scenario, ScenarioAction};

#[test]
fn reviewed_concurrent_scenarios_validate() {
    for source in [
        include_str!("../../../scenarios/kafka/concurrent-multi-producer.toml"),
        include_str!("../../../scenarios/kafka/concurrent-producer-consumer.toml"),
        include_str!("../../../scenarios/kafka/concurrent-two-client-pipeline.toml"),
    ] {
        let scenario: Scenario =
            toml::from_str(source).unwrap_or_else(|error| panic!("parse scenario: {error}"));
        scenario
            .validate()
            .unwrap_or_else(|error| panic!("validate scenario: {error}"));
    }
}

#[test]
fn concurrent_group_must_be_joined_before_handle_work_resumes() {
    let mut scenario = producer_consumer_scenario();
    let join = scenario
        .steps
        .iter()
        .position(|step| matches!(step.action, ScenarioAction::JoinConcurrentActors(_)))
        .unwrap_or_else(|| panic!("join step missing"));
    scenario.steps.remove(join);

    let error = match scenario.validate() {
        Ok(()) => panic!("unjoined concurrent group must fail"),
        Err(error) => error,
    };

    assert!(
        error
            .problems
            .iter()
            .any(|problem| problem.contains("was not joined"))
    );
    assert!(
        error
            .problems
            .iter()
            .any(|problem| problem.contains("while concurrent group"))
    );
}

#[test]
fn concurrent_actor_identities_are_unique() {
    let mut scenario = producer_consumer_scenario();
    let actors = scenario
        .steps
        .iter_mut()
        .find_map(|step| match &mut step.action {
            ScenarioAction::StartConcurrentActors(action) => Some(&mut action.actors),
            _ => None,
        });
    let Some(actors) = actors else {
        panic!("concurrent actors missing");
    };
    let first = actors[0].actor_id().clone();
    match &mut actors[1] {
        ConcurrentActor::ProducerSend { actor_id, .. }
        | ConcurrentActor::AssignedReceive { actor_id, .. } => *actor_id = first,
    }

    let error = match scenario.validate() {
        Ok(()) => panic!("duplicate actor identity must fail"),
        Err(error) => error,
    };

    assert!(
        error
            .problems
            .iter()
            .any(|problem| problem.contains("duplicate concurrent actor id"))
    );
}

#[test]
fn mismatched_join_preserves_the_active_group_diagnostic() {
    let mut scenario = producer_consumer_scenario();
    let join = scenario
        .steps
        .iter_mut()
        .find_map(|step| match &mut step.action {
            ScenarioAction::JoinConcurrentActors(action) => Some(action),
            _ => None,
        });
    let Some(join) = join else {
        panic!("concurrent join missing");
    };
    join.concurrency_id = ConcurrencyId::new("wrong-group")
        .unwrap_or_else(|error| panic!("valid replacement id: {error}"));

    let error = match scenario.validate() {
        Ok(()) => panic!("mismatched join must fail"),
        Err(error) => error,
    };

    assert!(
        error
            .problems
            .iter()
            .any(|problem| problem.contains("does not match active group"))
    );
    assert!(
        error
            .problems
            .iter()
            .any(|problem| problem.contains("was not joined"))
    );
}

#[test]
fn same_group_send_may_satisfy_a_concurrent_receive_expectation() {
    let scenario = producer_consumer_scenario();
    let start = scenario.steps.iter().find_map(|step| match &step.action {
        ScenarioAction::StartConcurrentActors(action) => Some(action),
        _ => None,
    });

    assert!(start.is_some_and(|action| {
        matches!(action.actors[0], ConcurrentActor::AssignedReceive { .. })
            && matches!(action.actors[1], ConcurrentActor::ProducerSend { .. })
    }));
    scenario
        .validate()
        .unwrap_or_else(|error| panic!("same-group expectation: {error}"));
}

fn producer_consumer_scenario() -> Scenario {
    toml::from_str(include_str!(
        "../../../scenarios/kafka/concurrent-producer-consumer.toml"
    ))
    .unwrap_or_else(|error| panic!("parse concurrent scenario: {error}"))
}
