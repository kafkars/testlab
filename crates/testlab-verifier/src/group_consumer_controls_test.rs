//! Group-consumer control tests cover exact issued and completed identities.

use testlab_schema::{
    AdapterCommand, AdapterEvent, AssignedStartPosition, GroupConsumerControl,
    GroupConsumerControlAction, GroupConsumerControlCommand, GroupConsumerControlCompletion,
    GroupConsumerControlKind, OperationId, Scenario, ScenarioAction, ScenarioStep, StepId,
    TopicPartitionIdentity,
};

use crate::index::HistoryIndex;
use crate::verify_fixture::{command, event};

#[test]
fn exact_group_control_command_and_completion_pass() {
    assert!(verify(false).is_empty());
}

#[test]
fn mismatched_group_control_completion_fails() {
    assert!(
        verify(true)
            .iter()
            .any(|violation| violation.contract_id.as_str() == "CONS-014")
    );
}

fn verify(mismatch: bool) -> Vec<testlab_schema::Violation> {
    let scenario = scenario();
    let ScenarioAction::ControlGroupConsumer(action) = &scenario.steps[7].action else {
        panic!("group control missing");
    };
    let history = vec![
        command(
            1,
            AdapterCommand::ControlGroupConsumer(GroupConsumerControlCommand {
                operation_id: action.operation_id.clone(),
                consumer_id: action.consumer_id.clone(),
                control: action.control.clone(),
                timeout_ms: action.timeout_ms,
            }),
        ),
        event(
            2,
            AdapterEvent::GroupConsumerControlCompleted(GroupConsumerControlCompletion {
                operation_id: action.operation_id.clone(),
                consumer_id: action.consumer_id.clone(),
                control: if mismatch {
                    GroupConsumerControlKind::Pause
                } else {
                    action.control.kind()
                },
            }),
        ),
    ];
    let index = HistoryIndex::build(&history);
    let mut violations = Vec::new();
    crate::group_consumer_controls::verify(&scenario, &index, &mut violations);
    violations
}

fn scenario() -> Scenario {
    let mut scenario: Scenario = toml::from_str(include_str!(
        "../../../scenarios/kafka/classic-group-round-trip.toml"
    ))
    .unwrap_or_else(|error| panic!("parse classic scenario: {error}"));
    let consumer_id = match &scenario.steps[6].action {
        ScenarioAction::CreateGroupConsumer { consumer_id, .. } => consumer_id.clone(),
        _ => panic!("group consumer fixture missing"),
    };
    scenario.steps.insert(
        7,
        ScenarioStep {
            id: StepId::new("control-group").unwrap_or_else(|error| panic!("step id: {error}")),
            action: ScenarioAction::ControlGroupConsumer(GroupConsumerControlAction {
                operation_id: OperationId::new("control-1")
                    .unwrap_or_else(|error| panic!("operation id: {error}")),
                consumer_id,
                control: GroupConsumerControl::Seek {
                    partition: TopicPartitionIdentity {
                        topic: "testlab-kafkars-classic-group".to_owned(),
                        partition: 0,
                    },
                    position: AssignedStartPosition::Beginning,
                },
                timeout_ms: 30_000,
            }),
        },
    );
    scenario
}
