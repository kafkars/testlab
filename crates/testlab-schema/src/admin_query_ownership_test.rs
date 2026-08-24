//! Read-only admin ownership tests cover client state, identities, and capability use.

use std::collections::{BTreeMap, BTreeSet};

use super::{
    AdminOffsetPosition, Capability, ClientId, OperationId, SCENARIO_SCHEMA_VERSION, Scenario,
    ScenarioAction, ScenarioId, ScenarioStep, StepId,
};
use crate::admin_action_validation::validate;

#[test]
fn admin_queries_require_a_live_client_and_shared_operation_identity() {
    let duplicate = operation("admin-shared");
    let shutdown = client("shutdown-client");
    let clients = BTreeMap::from([(shutdown.clone(), true)]);
    let mut operation_ids = BTreeSet::from([duplicate.clone()]);
    let mut problems = Vec::new();

    validate(
        &list_topics(
            client("missing-client"),
            duplicate.clone(),
            vec!["records".to_owned()],
        ),
        &clients,
        &mut operation_ids,
        &mut problems,
    );
    validate(
        &list_offsets(shutdown, duplicate, 0, 0),
        &clients,
        &mut operation_ids,
        &mut problems,
    );

    assert_problem(&problems, "uses missing client");
    assert_problem(&problems, "uses shut down client");
    assert_eq!(
        problems
            .iter()
            .filter(|problem| problem.contains("duplicate operation id"))
            .count(),
        2
    );
}

#[test]
fn admin_queries_require_the_admin_capability() {
    let client_id = client("client-1");
    let mut scenario = Scenario {
        schema_version: SCENARIO_SCHEMA_VERSION,
        id: ScenarioId::new("admin.query-capability")
            .unwrap_or_else(|error| panic!("scenario id: {error}")),
        title: "admin query capability".to_owned(),
        description: "read-only admin queries require explicit support".to_owned(),
        timeout_ms: 1_000,
        requires: BTreeSet::from([Capability::Lifecycle]),
        steps: vec![
            step(
                "create-client",
                ScenarioAction::CreateClient {
                    client_id: client_id.clone(),
                },
            ),
            step(
                "describe-topic",
                describe_topic(client_id.clone(), operation("admin-describe")),
            ),
            step(
                "list-topics",
                list_topics(
                    client_id.clone(),
                    operation("admin-topics"),
                    vec!["records".to_owned()],
                ),
            ),
            step(
                "list-offsets",
                list_offsets(client_id.clone(), operation("admin-offset"), 0, 0),
            ),
            step(
                "shutdown-client",
                ScenarioAction::ShutdownClient { client_id },
            ),
        ],
        assertions: Vec::new(),
    };

    let error = match scenario.validate() {
        Ok(()) => panic!("missing admin capability must fail validation"),
        Err(error) => error,
    };
    assert_problem(&error.problems, "admin steps require the admin capability");

    scenario.requires.insert(Capability::Admin);
    scenario
        .validate()
        .unwrap_or_else(|error| panic!("valid admin query scenario: {error}"));
}

fn describe_topic(client_id: ClientId, operation_id: OperationId) -> ScenarioAction {
    ScenarioAction::DescribeTopic {
        client_id,
        operation_id,
        topic: "records".to_owned(),
        expected_partitions: vec![0],
        timeout_ms: 1_000,
    }
}

fn list_topics(
    client_id: ClientId,
    operation_id: OperationId,
    required_topics: Vec<String>,
) -> ScenarioAction {
    ScenarioAction::ListTopics {
        client_id,
        operation_id,
        include_internal: false,
        required_topics,
        timeout_ms: 1_000,
    }
}

fn list_offsets(
    client_id: ClientId,
    operation_id: OperationId,
    partition: i32,
    expected_offset: i64,
) -> ScenarioAction {
    ScenarioAction::ListOffsets {
        client_id,
        operation_id,
        topic: "records".to_owned(),
        partition,
        position: AdminOffsetPosition::Latest,
        expected_offset,
        timeout_ms: 1_000,
    }
}

fn assert_problem(problems: &[String], expected: &str) {
    assert!(
        problems.iter().any(|problem| problem.contains(expected)),
        "missing {expected:?} in {problems:?}"
    );
}

fn client(value: &str) -> ClientId {
    ClientId::new(value).unwrap_or_else(|error| panic!("client id: {error}"))
}

fn operation(value: &str) -> OperationId {
    OperationId::new(value).unwrap_or_else(|error| panic!("operation id: {error}"))
}

fn step(value: &str, action: ScenarioAction) -> ScenarioStep {
    ScenarioStep {
        id: StepId::new(value).unwrap_or_else(|error| panic!("step id: {error}")),
        action,
    }
}
