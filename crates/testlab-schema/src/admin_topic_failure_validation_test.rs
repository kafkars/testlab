//! Duplicate-topic expectations require an exact prior creation and normalized error.

use std::collections::BTreeSet;

use super::{
    Capability, ClientId, CreateTopicAction, OperationId, SCENARIO_SCHEMA_VERSION, Scenario,
    ScenarioAction, ScenarioId, ScenarioStep, StepId, TOPIC_ALREADY_EXISTS_ERROR_CODE,
};

#[test]
fn duplicate_creation_accepts_an_identical_prior_success() {
    let scenario = scenario(vec![create("create", 2, None), duplicate("duplicate", 2)]);

    scenario
        .validate()
        .unwrap_or_else(|error| panic!("valid duplicate creation: {error}"));
}

#[test]
fn duplicate_creation_requires_a_prior_identical_success() {
    for actions in [
        vec![duplicate("duplicate", 2)],
        vec![create("create", 1, None), duplicate("duplicate", 2)],
    ] {
        let problems = problems(&scenario(actions));

        assert!(
            problems
                .iter()
                .any(|problem| problem.contains("prior identical successful topic creation")),
            "{problems:?}"
        );
    }
}

#[test]
fn duplicate_creation_requires_the_exact_normalized_code() {
    let actions = vec![
        create("create", 2, None),
        create("duplicate", 2, Some("broker")),
    ];
    let problems = problems(&scenario(actions));

    assert!(
        problems
            .iter()
            .any(|problem| problem.contains(TOPIC_ALREADY_EXISTS_ERROR_CODE)),
        "{problems:?}"
    );
}

#[test]
fn duplicate_creation_must_be_the_final_public_operation() {
    let actions = vec![
        create("create", 2, None),
        duplicate("duplicate", 2),
        create("after-failure", 2, None),
    ];
    let problems = problems(&scenario(actions));

    assert!(
        problems
            .iter()
            .any(|problem| problem.contains("followed only by client shutdown")),
        "{problems:?}"
    );
}

fn scenario(actions: Vec<ScenarioAction>) -> Scenario {
    let client_id = client();
    let mut steps = vec![step(
        "create-client",
        ScenarioAction::CreateClient {
            client_id: client_id.clone(),
        },
    )];
    steps.extend(
        actions
            .into_iter()
            .enumerate()
            .map(|(index, action)| step(&format!("admin-{index}"), action)),
    );
    steps.push(step(
        "shutdown-client",
        ScenarioAction::ShutdownClient { client_id },
    ));
    Scenario {
        schema_version: SCENARIO_SCHEMA_VERSION,
        id: ScenarioId::new("admin.duplicate-topic-validation")
            .unwrap_or_else(|error| panic!("scenario id: {error}")),
        title: "duplicate topic validation".to_owned(),
        description: "duplicate creation requires exact prior public state".to_owned(),
        timeout_ms: 60_000,
        requires: BTreeSet::from([Capability::Admin, Capability::Lifecycle]),
        steps,
        assertions: Vec::new(),
    }
}

fn create(
    operation_id: &str,
    partitions: i32,
    expected_error_code: Option<&str>,
) -> ScenarioAction {
    ScenarioAction::CreateTopic(CreateTopicAction {
        client_id: client(),
        operation_id: OperationId::new(operation_id)
            .unwrap_or_else(|error| panic!("operation id: {error}")),
        topic: "orders".to_owned(),
        partitions,
        replication_factor: 1,
        validate_only: false,
        expected_error_code: expected_error_code.map(str::to_owned),
        timeout_ms: 1_000,
    })
}

fn duplicate(operation_id: &str, partitions: i32) -> ScenarioAction {
    create(
        operation_id,
        partitions,
        Some(TOPIC_ALREADY_EXISTS_ERROR_CODE),
    )
}

fn problems(scenario: &Scenario) -> Vec<String> {
    let Err(error) = scenario.validate() else {
        panic!("scenario must fail validation");
    };
    error.problems
}

fn step(value: &str, action: ScenarioAction) -> ScenarioStep {
    ScenarioStep {
        id: StepId::new(value).unwrap_or_else(|error| panic!("step id: {error}")),
        action,
    }
}

fn client() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client id: {error}"))
}
