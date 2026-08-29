//! `DeleteRecords` target tests keep independent seeding isolated from scenario writes.

use super::{
    CreateTopicAction, OperationId, ProducerId, RecordSpec, Scenario, ScenarioAction, StepId,
};

#[test]
fn delete_records_requires_a_harness_owned_topic() {
    let mut scenario = fixture();
    let ScenarioAction::DeleteRecords(action) = &scenario.steps[4].action else {
        panic!("delete records fixture shape");
    };
    scenario.steps.insert(
        2,
        super::ScenarioStep {
            id: step("subject-create"),
            action: ScenarioAction::CreateTopic(CreateTopicAction {
                client_id: action.client_id.clone(),
                operation_id: operation("subject-create"),
                topic: action.topic.clone(),
                partitions: 1,
                replication_factor: 1,
                validate_only: false,
                expected_error_code: None,
                timeout_ms: 1_000,
            }),
        },
    );

    assert_problem(&scenario, "requires a harness-owned topic");
}

#[test]
fn delete_records_rejects_scenario_writes_to_the_seeded_partition() {
    let mut scenario = fixture();
    let ScenarioAction::DeleteRecords(action) = &scenario.steps[4].action else {
        panic!("delete records fixture shape");
    };
    scenario.steps.insert(
        4,
        super::ScenarioStep {
            id: step("target-write"),
            action: ScenarioAction::Send {
                producer_id: ProducerId::new("producer-1")
                    .unwrap_or_else(|error| panic!("producer id: {error}")),
                operation_id: operation("target-write"),
                record: RecordSpec {
                    topic: action.topic.clone(),
                    partition: action.partition,
                    sequence: 1,
                    key: None,
                    value: None,
                    headers: Vec::new(),
                },
            },
        },
    );

    assert_problem(
        &scenario,
        "requires a partition without scenario record writes",
    );
}

#[test]
fn delete_records_target_may_appear_only_once() {
    let mut scenario = fixture();
    let mut repeated = scenario.steps[4].clone();
    repeated.id = step("delete-prefix-again");
    let ScenarioAction::DeleteRecords(action) = &mut repeated.action else {
        panic!("delete records fixture shape");
    };
    action.operation_id = operation("delete-prefix-again");
    scenario.steps.insert(5, repeated);

    assert_problem(&scenario, "repeats a delete-records target");
}

fn fixture() -> Scenario {
    toml::from_str(include_str!(
        "../../../scenarios/kafka/admin-delete-records.toml"
    ))
    .unwrap_or_else(|error| panic!("parse delete records scenario: {error}"))
}

fn assert_problem(scenario: &Scenario, expected: &str) {
    let error = match scenario.validate() {
        Ok(()) => panic!("invalid delete records target must fail"),
        Err(error) => error,
    };
    assert!(
        error.problems.iter().any(|value| value.contains(expected)),
        "missing {expected:?} in {:?}",
        error.problems
    );
}

fn operation(value: &str) -> OperationId {
    OperationId::new(value).unwrap_or_else(|error| panic!("operation id: {error}"))
}

fn step(value: &str) -> StepId {
    StepId::new(value).unwrap_or_else(|error| panic!("step id: {error}"))
}
