//! Ownership protocol tests preserve expectations only on the harness side.

use crate::{
    AdapterCommand, AdapterEvent, AssignBeginningBatchAction, AssignBeginningBatchCommand,
    ConsumerId, GroupAssignmentTransition, GroupAssignmentTransitionKind,
    GroupAssignmentsObservation, GroupConsumerAssignment, GroupMembershipEpoch,
    ObserveGroupAssignmentsAction, ObserveGroupAssignmentsCommand, OperationId, ScenarioAction,
    TopicPartitionIdentity,
};

#[test]
fn observation_expectations_do_not_cross_the_wire_boundary() {
    let operation_id = id(OperationId::new("assignment-observation-1"));
    let consumer_ids = vec![id(ConsumerId::new("consumer-1"))];
    let partitions = vec![partition("orders", 0), partition("orders", 1)];
    let action = ScenarioAction::ObserveGroupAssignments(ObserveGroupAssignmentsAction {
        operation_id: operation_id.clone(),
        consumer_ids: consumer_ids.clone(),
        partitions,
        timeout_ms: 30_000,
    });
    let command = AdapterCommand::ObserveGroupAssignments(ObserveGroupAssignmentsCommand {
        operation_id,
        consumer_ids,
        timeout_ms: 30_000,
    });

    let action_json =
        serde_json::to_value(action).unwrap_or_else(|error| panic!("serialize action: {error}"));
    let command_json =
        serde_json::to_value(command).unwrap_or_else(|error| panic!("serialize command: {error}"));

    assert!(action_json.get("partitions").is_some());
    assert!(command_json.get("partitions").is_none());
}

#[test]
fn multi_partition_assignment_round_trips_exactly() {
    let action = ScenarioAction::AssignBeginningBatch(AssignBeginningBatchAction {
        consumer_id: id(ConsumerId::new("consumer-1")),
        partitions: vec![partition("orders", 0), partition("orders", 2)],
        timeout_ms: 20_000,
    });
    let json = serde_json::to_string(&action)
        .unwrap_or_else(|error| panic!("serialize assignment: {error}"));
    let parsed: ScenarioAction = serde_json::from_str(&json)
        .unwrap_or_else(|error| panic!("deserialize assignment: {error}"));

    assert_eq!(parsed, action);

    let ScenarioAction::AssignBeginningBatch(action) = parsed else {
        panic!("expected batch assignment");
    };
    let command = AssignBeginningBatchCommand {
        consumer_id: action.consumer_id,
        partitions: action.partitions,
        timeout_ms: action.timeout_ms,
    };
    assert_eq!(command.partitions.len(), 2);
}

#[test]
fn assignment_evidence_retains_member_and_transition_fences() {
    let consumer_id = id(ConsumerId::new("consumer-1"));
    let operation_id = id(OperationId::new("assignment-observation-1"));
    let observation = GroupAssignmentsObservation {
        operation_id,
        transitions: vec![GroupAssignmentTransition {
            consumer_id: consumer_id.clone(),
            kind: GroupAssignmentTransitionKind::Assigned,
            assignment_epoch: 7,
            partitions: vec![partition("orders", 0)],
        }],
        assignments: vec![GroupConsumerAssignment {
            consumer_id,
            group_id: "orders-group".to_owned(),
            member_id: "broker-member-1".to_owned(),
            group_epoch: GroupMembershipEpoch::Classic { generation_id: 3 },
            assignment_epoch: 7,
            partitions: vec![partition("orders", 0)],
        }],
    };
    let event = AdapterEvent::GroupAssignmentsObserved(observation);
    let json = serde_json::to_string(&event)
        .unwrap_or_else(|error| panic!("serialize observation: {error}"));
    let parsed: AdapterEvent = serde_json::from_str(&json)
        .unwrap_or_else(|error| panic!("deserialize observation: {error}"));

    assert_eq!(parsed, event);
}

#[test]
fn all_seven_ownership_and_recovery_scenarios_validate() {
    for source in [
        include_str!("../../../scenarios/kafka/assigned-consumer-multi-partition.toml"),
        include_str!("../../../scenarios/kafka/classic-group-membership-ownership.toml"),
        include_str!("../../../scenarios/kafka/consumer-protocol-group-membership-ownership.toml"),
        include_str!("../../../scenarios/kafka/classic-group-offset-resume.toml"),
        include_str!("../../../scenarios/kafka/consumer-protocol-group-offset-resume.toml"),
        include_str!("../../../scenarios/kafka/classic-group-session-recovery.toml"),
        include_str!("../../../scenarios/kafka/consumer-protocol-group-session-recovery.toml"),
    ] {
        let scenario: crate::Scenario = toml::from_str(source)
            .unwrap_or_else(|error| panic!("parse ownership scenario: {error}"));
        scenario
            .validate()
            .unwrap_or_else(|error| panic!("validate ownership scenario: {error}"));
    }
}

fn partition(topic: &str, partition: i32) -> TopicPartitionIdentity {
    TopicPartitionIdentity {
        topic: topic.to_owned(),
        partition,
    }
}

fn id<T, E>(result: Result<T, E>) -> T
where
    E: std::fmt::Display,
{
    result.unwrap_or_else(|error| panic!("fixture id: {error}"))
}
