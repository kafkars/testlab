use crate::{
    AdapterCommand, CreateTopicCommand, Scenario, ScenarioAction, TopicReplicaAssignmentSpec,
};

#[test]
fn checked_in_manual_topic_placement_is_valid() {
    checked_in_scenario()
        .validate()
        .unwrap_or_else(|error| panic!("validate manual topic placement: {error}"));
}

#[test]
fn manual_topic_placement_round_trips_on_action_and_wire() {
    let scenario = checked_in_scenario();
    let action = scenario
        .steps
        .iter()
        .find_map(|step| match &step.action {
            ScenarioAction::CreateTopic(action) => Some(action.clone()),
            _ => None,
        })
        .unwrap_or_else(|| panic!("manual create-topic action"));
    let command = CreateTopicCommand {
        client_id: action.client_id.clone(),
        operation_id: action.operation_id.clone(),
        topic: action.topic.clone(),
        partitions: action.partitions,
        replication_factor: action.replication_factor,
        replica_assignments: action.replica_assignments.clone(),
        configs: Vec::new(),
        validate_only: action.validate_only,
        timeout_ms: action.timeout_ms,
    };

    round_trip(&ScenarioAction::CreateTopic(action));
    round_trip(&AdapterCommand::CreateTopic(command));
}

#[test]
fn manual_topic_placement_rejects_detached_or_malformed_assignments() {
    let mut missing = checked_in_scenario();
    manual_action(&mut missing).replica_assignments = Some(vec![assignment(0, &[1, 2])]);
    assert_problem(&missing, "exactly one entry per partition");

    let mut detached = checked_in_scenario();
    manual_action(&mut detached).replica_assignments = Some(vec![
        assignment(0, &[1, 2]),
        assignment(2, &[2, 3]),
        assignment(1, &[3, 1]),
    ]);
    assert_problem(&detached, "contiguous caller order from partition 0");

    let mut duplicate = checked_in_scenario();
    manual_action(&mut duplicate).replica_assignments = Some(vec![
        assignment(0, &[1, 1]),
        assignment(1, &[2, 3]),
        assignment(2, &[3, 1]),
    ]);
    assert_problem(&duplicate, "broker_ids must be unique nonnegative IDs");

    let mut wrong_factor = checked_in_scenario();
    manual_action(&mut wrong_factor).replica_assignments = Some(vec![
        assignment(0, &[1]),
        assignment(1, &[2, 3]),
        assignment(2, &[3, 1]),
    ]);
    assert_problem(&wrong_factor, "broker_ids must match replication_factor");
}

fn manual_action(scenario: &mut Scenario) -> &mut crate::CreateTopicAction {
    scenario
        .steps
        .iter_mut()
        .find_map(|step| match &mut step.action {
            ScenarioAction::CreateTopic(action) => Some(action),
            _ => None,
        })
        .unwrap_or_else(|| panic!("manual create-topic action"))
}

fn assignment(partition_index: i32, broker_ids: &[i32]) -> TopicReplicaAssignmentSpec {
    TopicReplicaAssignmentSpec {
        partition_index,
        broker_ids: broker_ids.to_vec(),
    }
}

fn checked_in_scenario() -> Scenario {
    toml::from_str(include_str!(
        "../../../scenarios/kafka/admin-create-topic-manual-replica-placement.toml"
    ))
    .unwrap_or_else(|error| panic!("parse manual topic placement: {error}"))
}

fn assert_problem(scenario: &Scenario, expected: &str) {
    let error = scenario
        .validate()
        .expect_err("malformed manual placement must fail");
    assert!(
        error
            .problems
            .iter()
            .any(|problem| problem.contains(expected)),
        "missing {expected:?} in {:?}",
        error.problems
    );
}

fn round_trip<T>(value: &T)
where
    T: serde::Serialize + serde::de::DeserializeOwned + Eq + std::fmt::Debug,
{
    let encoded = serde_json::to_vec(value)
        .unwrap_or_else(|error| panic!("serialize manual placement: {error}"));
    let decoded = serde_json::from_slice(&encoded)
        .unwrap_or_else(|error| panic!("deserialize manual placement: {error}"));
    assert_eq!(*value, decoded);
}
