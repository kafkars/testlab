use crate::{AdapterCommand, CreatePartitionsCommand, Scenario, ScenarioAction};

#[test]
fn checked_in_manual_partition_expansion_is_valid() {
    checked_in_scenario()
        .validate()
        .unwrap_or_else(|error| panic!("validate manual partition expansion: {error}"));
}

#[test]
fn manual_partition_expansion_round_trips_on_action_and_wire() {
    let scenario = checked_in_scenario();
    let action = scenario
        .steps
        .iter()
        .find_map(|step| match &step.action {
            ScenarioAction::CreatePartitions(action) => Some(action.clone()),
            _ => None,
        })
        .unwrap_or_else(|| panic!("manual create-partitions action"));
    let command = CreatePartitionsCommand {
        client_id: action.client_id.clone(),
        operation_id: action.operation_id.clone(),
        topic: action.topic.clone(),
        total_count: action.total_count,
        replica_assignments: action.replica_assignments.clone(),
        validate_only: action.validate_only,
        timeout_ms: action.timeout_ms,
    };

    round_trip(&ScenarioAction::CreatePartitions(action));
    round_trip(&AdapterCommand::CreatePartitions(command));
}

#[test]
fn manual_partition_expansion_rejects_malformed_assignments() {
    let mut without_count = checked_in_scenario();
    manual_action(&mut without_count).expected_current_count = None;
    assert_problem(&without_count, "expected_current_count exactly when");

    let mut missing = checked_in_scenario();
    manual_action(&mut missing).replica_assignments = Some(vec![vec![3, 1]]);
    assert_problem(&missing, "exactly one entry per newly added partition");

    let mut duplicate = checked_in_scenario();
    manual_action(&mut duplicate).replica_assignments = Some(vec![vec![3, 3], vec![2, 3]]);
    assert_problem(&duplicate, "broker_ids must be unique nonnegative IDs");

    let mut empty = checked_in_scenario();
    manual_action(&mut empty).replica_assignments = Some(vec![Vec::new(), vec![2, 3]]);
    assert_problem(&empty, "broker_ids must contain 1 to 100 entries");

    let mut detached = checked_in_scenario();
    manual_action(&mut detached).expected_current_count = Some(2);
    assert_problem(&detached, "prior exact partition count of 2");
}

fn manual_action(scenario: &mut Scenario) -> &mut crate::CreatePartitionsAction {
    scenario
        .steps
        .iter_mut()
        .find_map(|step| match &mut step.action {
            ScenarioAction::CreatePartitions(action) => Some(action),
            _ => None,
        })
        .unwrap_or_else(|| panic!("manual create-partitions action"))
}

fn checked_in_scenario() -> Scenario {
    toml::from_str(include_str!(
        "../../../scenarios/kafka/admin-create-partitions-manual-replica-placement.toml"
    ))
    .unwrap_or_else(|error| panic!("parse manual partition expansion: {error}"))
}

fn assert_problem(scenario: &Scenario, expected: &str) {
    let error = scenario
        .validate()
        .expect_err("malformed manual partition expansion must fail");
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
        .unwrap_or_else(|error| panic!("serialize manual partition expansion: {error}"));
    let decoded = serde_json::from_slice(&encoded)
        .unwrap_or_else(|error| panic!("deserialize manual partition expansion: {error}"));
    assert_eq!(*value, decoded);
}
