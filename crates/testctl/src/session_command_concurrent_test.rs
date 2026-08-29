//! Concurrent translation tests keep scheduling membership exact and expectations private.

use testlab_schema::{AdapterCommand, ConcurrentActorCommand, Scenario, ScenarioAction};

use crate::runner_protocol::ExpectedEvent;

#[test]
fn start_translation_strips_receive_expectations() {
    let scenario = scenario();
    let action = scenario.steps.iter().find_map(|step| match &step.action {
        ScenarioAction::StartConcurrentActors(_) => Some(&step.action),
        _ => None,
    });
    let Some(action) = action else {
        panic!("start action missing");
    };

    let Some((AdapterCommand::StartConcurrentActors(command), expected)) =
        crate::session_command_concurrent::translate(action, &scenario)
    else {
        panic!("start action must translate");
    };

    assert!(matches!(
        command.actors.first(),
        Some(ConcurrentActorCommand::AssignedReceive {
            receive_id,
            timeout_ms: 30_000,
            ..
        }) if receive_id.as_str() == "receive-concurrent"
    ));
    assert!(matches!(
        expected,
        ExpectedEvent::ConcurrentActorsStarted(_)
    ));
}

#[test]
fn join_translation_recovers_exact_started_membership() {
    let scenario = scenario();
    let action = scenario.steps.iter().find_map(|step| match &step.action {
        ScenarioAction::JoinConcurrentActors(_) => Some(&step.action),
        _ => None,
    });
    let Some(action) = action else {
        panic!("join action missing");
    };

    let Some((AdapterCommand::JoinConcurrentActors { concurrency_id, .. }, expected)) =
        crate::session_command_concurrent::translate(action, &scenario)
    else {
        panic!("join action must translate");
    };

    assert_eq!(concurrency_id.as_str(), "produce-consume");
    let ExpectedEvent::ConcurrentActorsCompleted(expected) = expected else {
        panic!("join expectation missing");
    };
    assert_eq!(expected.actors.len(), 2);
    assert_eq!(expected.receives.len(), 1);
    assert_eq!(expected.sends.len(), 1);
}

fn scenario() -> Scenario {
    toml::from_str(include_str!(
        "../../../scenarios/kafka/concurrent-producer-consumer.toml"
    ))
    .unwrap_or_else(|error| panic!("parse concurrent scenario: {error}"))
}
