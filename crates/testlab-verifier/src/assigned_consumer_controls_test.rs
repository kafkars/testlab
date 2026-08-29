//! Assigned-consumer control tests cover exact issued and completed identities.

use testlab_schema::{
    AdapterCommand, AdapterEvent, AssignedConsumerControl, AssignedConsumerControlCommand,
    AssignedConsumerControlCompletion, AssignedConsumerControlKind, AssignedStartPosition,
    Scenario, ScenarioAction, TopicPartitionIdentity,
};

use crate::index::HistoryIndex;
use crate::verify_fixture::{command, event};

#[test]
fn exact_control_command_and_completion_pass() {
    assert!(verify(false).is_empty());
}

#[test]
fn mismatched_control_completion_fails() {
    let violations = verify(true);
    assert!(
        violations
            .iter()
            .any(|violation| violation.contract_id.as_str() == "CONS-013")
    );
}

fn verify(mismatch: bool) -> Vec<testlab_schema::Violation> {
    let scenario = scenario();
    let ScenarioAction::ControlAssignedConsumer(action) = &scenario.steps[7].action else {
        panic!("assigned control missing");
    };
    let command_payload = AssignedConsumerControlCommand {
        operation_id: action.operation_id.clone(),
        consumer_id: action.consumer_id.clone(),
        control: action.control.clone(),
        timeout_ms: action.timeout_ms,
    };
    let completion = AssignedConsumerControlCompletion {
        operation_id: action.operation_id.clone(),
        consumer_id: action.consumer_id.clone(),
        control: if mismatch {
            AssignedConsumerControlKind::Pause
        } else {
            action.control.kind()
        },
    };
    let history = vec![
        command(1, AdapterCommand::ControlAssignedConsumer(command_payload)),
        event(
            2,
            AdapterEvent::AssignedConsumerControlCompleted(completion),
        ),
    ];
    let index = HistoryIndex::build(&history);
    let mut violations = Vec::new();
    crate::assigned_consumer_controls::verify(&scenario, &index, &mut violations);
    violations
}

fn scenario() -> Scenario {
    let mut scenario: Scenario = toml::from_str(include_str!(
        "../../../scenarios/kafka/assigned-consumer-round-trip.toml"
    ))
    .unwrap_or_else(|error| panic!("parse assigned scenario: {error}"));
    let ScenarioAction::AssignBeginning {
        consumer_id,
        topic,
        partition,
    } = &scenario.steps[7].action
    else {
        panic!("assignment fixture missing");
    };
    scenario.steps[7].action =
        ScenarioAction::ControlAssignedConsumer(testlab_schema::AssignedConsumerControlAction {
            operation_id: testlab_schema::OperationId::new("control-1")
                .unwrap_or_else(|error| panic!("operation id: {error}")),
            consumer_id: consumer_id.clone(),
            control: AssignedConsumerControl::Seek {
                partition: TopicPartitionIdentity {
                    topic: topic.clone(),
                    partition: *partition,
                },
                position: AssignedStartPosition::Beginning,
            },
            timeout_ms: 20_000,
        });
    scenario
}
