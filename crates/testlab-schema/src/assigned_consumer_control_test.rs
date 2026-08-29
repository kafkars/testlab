//! Assigned-consumer control tests pin wire shape, capability, and validation.

use crate::{
    AssignedConsumerControl, AssignedConsumerControlAction, AssignedStartPosition, Capability,
    OperationId, Scenario, ScenarioAction, TopicPartitionIdentity,
};

#[test]
fn direct_control_round_trips_and_validates() {
    let scenario = controlled_scenario();
    scenario
        .validate()
        .unwrap_or_else(|error| panic!("validate assigned control: {error}"));
    let encoded = toml::to_string(&scenario)
        .unwrap_or_else(|error| panic!("serialize assigned control: {error}"));
    let decoded: Scenario = toml::from_str(&encoded)
        .unwrap_or_else(|error| panic!("deserialize assigned control: {error}"));
    assert_eq!(decoded, scenario);
}

#[test]
fn direct_control_rejects_missing_capability_duplicate_identity_and_negative_offset() {
    let mut scenario = controlled_scenario();
    scenario
        .requires
        .remove(&Capability::AssignedConsumerControls);
    assert_problem(&scenario, "assigned_consumer_controls capability");
    scenario
        .requires
        .insert(Capability::AssignedConsumerControls);
    let ScenarioAction::ControlAssignedConsumer(action) = &mut scenario.steps[7].action else {
        panic!("assigned control missing");
    };
    action.operation_id =
        OperationId::new("op-produced").unwrap_or_else(|error| panic!("operation id: {error}"));
    action.control = AssignedConsumerControl::Seek {
        partition: TopicPartitionIdentity {
            topic: "topic".to_owned(),
            partition: 0,
        },
        position: AssignedStartPosition::Offset { offset: -1 },
    };
    assert_problem(&scenario, "duplicate operation id");
    assert_problem(&scenario, "negative offset");
}

fn controlled_scenario() -> Scenario {
    let mut scenario: Scenario = toml::from_str(include_str!(
        "../../../scenarios/kafka/assigned-consumer-round-trip.toml"
    ))
    .unwrap_or_else(|error| panic!("parse assigned scenario: {error}"));
    scenario
        .requires
        .insert(Capability::AssignedConsumerControls);
    let ScenarioAction::AssignBeginning {
        consumer_id,
        topic,
        partition,
    } = &scenario.steps[7].action
    else {
        panic!("assignment fixture missing");
    };
    scenario.steps[7].action =
        ScenarioAction::ControlAssignedConsumer(AssignedConsumerControlAction {
            operation_id: OperationId::new("control-1")
                .unwrap_or_else(|error| panic!("operation id: {error}")),
            consumer_id: consumer_id.clone(),
            control: AssignedConsumerControl::Replace {
                partitions: vec![crate::AssignedPartitionPosition {
                    topic: topic.clone(),
                    partition: *partition,
                    position: AssignedStartPosition::Beginning,
                }],
            },
            timeout_ms: 20_000,
        });
    scenario
}

fn assert_problem(scenario: &Scenario, expected: &str) {
    let error = match scenario.validate() {
        Ok(()) => panic!("assigned control fixture must be invalid"),
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
