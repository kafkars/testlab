//! Group-consumer shutdown tests cover exact commands, correlation, and request counts.

use testlab_schema::{
    AdapterCommand, AdapterEvent, GroupConsumerShutdownCommand, GroupConsumerShutdownCompletion,
    Scenario, ScenarioAction,
};

use crate::index::HistoryIndex;
use crate::verify_fixture::{command, event};

#[test]
fn exact_group_shutdown_command_and_completion_pass() {
    assert!(verify(false).is_empty());
}

#[test]
fn mismatched_group_shutdown_completion_fails() {
    assert!(
        verify(true)
            .iter()
            .any(|violation| violation.contract_id.as_str() == "LIFE-015")
    );
}

fn verify(mismatch: bool) -> Vec<testlab_schema::Violation> {
    let scenario = scenario();
    let ScenarioAction::ShutdownGroupConsumer(action) = &scenario.steps[8].action else {
        panic!("group shutdown missing");
    };
    let history = vec![
        command(
            1,
            AdapterCommand::ShutdownGroupConsumer(GroupConsumerShutdownCommand {
                operation_id: action.operation_id.clone(),
                consumer_id: action.consumer_id.clone(),
                request_count: action.request_count,
                timeout_ms: action.timeout_ms,
            }),
        ),
        event(
            2,
            AdapterEvent::GroupConsumerShutdownCompleted(GroupConsumerShutdownCompletion {
                operation_id: action.operation_id.clone(),
                consumer_id: action.consumer_id.clone(),
                request_count: action.request_count + u8::from(mismatch),
            }),
        ),
    ];
    let index = HistoryIndex::build(&history);
    let mut violations = Vec::new();
    crate::group_consumer_shutdown::verify(&scenario, &index, &mut violations);
    violations
}

fn scenario() -> Scenario {
    toml::from_str(include_str!(
        "../../../scenarios/kafka/classic-group-shutdown.toml"
    ))
    .unwrap_or_else(|error| panic!("parse group shutdown scenario: {error}"))
}
