//! Group-consumer control tests pin wire shape, capability, and ownership validation.

use crate::{
    AssignedStartPosition, Capability, GroupConsumerControl, GroupConsumerControlAction,
    OperationId, Scenario, ScenarioAction, TopicPartitionIdentity,
};

#[test]
fn hosted_group_control_round_trips_and_validates() {
    let scenario = controlled_scenario();
    scenario
        .validate()
        .unwrap_or_else(|error| panic!("validate group control: {error}"));
    let encoded = toml::to_string(&scenario)
        .unwrap_or_else(|error| panic!("serialize group control: {error}"));
    let decoded: Scenario = toml::from_str(&encoded)
        .unwrap_or_else(|error| panic!("deserialize group control: {error}"));
    assert_eq!(decoded, scenario);
}

#[test]
fn hosted_group_control_requires_capability_and_unique_nonnegative_identity() {
    let mut scenario = controlled_scenario();
    scenario.requires.remove(&Capability::GroupConsumerControls);
    assert_problem(&scenario, "group_consumer_controls capability");
    scenario.requires.insert(Capability::GroupConsumerControls);
    let ScenarioAction::ControlGroupConsumer(action) = &mut scenario.steps[7].action else {
        panic!("group control missing");
    };
    action.operation_id =
        OperationId::new("op-produced").unwrap_or_else(|error| panic!("operation id: {error}"));
    action.control = GroupConsumerControl::Seek {
        partition: partition(),
        position: AssignedStartPosition::Offset { offset: -1 },
    };
    assert_problem(&scenario, "duplicate operation id");
    assert_problem(&scenario, "negative offset");
}

fn controlled_scenario() -> Scenario {
    let mut scenario: Scenario = toml::from_str(include_str!(
        "../../../scenarios/kafka/classic-group-round-trip.toml"
    ))
    .unwrap_or_else(|error| panic!("parse classic scenario: {error}"));
    scenario.requires.insert(Capability::GroupConsumerControls);
    let consumer_id = match &scenario.steps[6].action {
        ScenarioAction::CreateGroupConsumer { consumer_id, .. } => consumer_id.clone(),
        _ => panic!("group consumer fixture missing"),
    };
    scenario.steps.insert(
        7,
        crate::ScenarioStep {
            id: crate::StepId::new("group-control")
                .unwrap_or_else(|error| panic!("step id: {error}")),
            action: ScenarioAction::ControlGroupConsumer(GroupConsumerControlAction {
                operation_id: OperationId::new("control-1")
                    .unwrap_or_else(|error| panic!("operation id: {error}")),
                consumer_id,
                control: GroupConsumerControl::Seek {
                    partition: partition(),
                    position: AssignedStartPosition::Beginning,
                },
                timeout_ms: 30_000,
            }),
        },
    );
    scenario
}

fn partition() -> TopicPartitionIdentity {
    TopicPartitionIdentity {
        topic: "testlab-kafkars-classic-group".to_owned(),
        partition: 0,
    }
}

fn assert_problem(scenario: &Scenario, expected: &str) {
    let error = match scenario.validate() {
        Ok(()) => panic!("group control fixture must be invalid"),
        Err(error) => error,
    };
    assert!(
        error
            .problems
            .iter()
            .any(|problem| problem.contains(expected)),
        "missing {expected:?} in {:?}",
        error.problems
    );
}
