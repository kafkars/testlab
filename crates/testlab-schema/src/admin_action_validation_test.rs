//! Partition-administration validation tests cover ownership, identity, and bounds.

use std::collections::{BTreeMap, BTreeSet};

use super::{
    Capability, ClientId, CreatePartitionsAction, CreateTopicAction, OperationId,
    SCENARIO_SCHEMA_VERSION, Scenario, ScenarioAction, ScenarioId, ScenarioStep, StepId,
};
use crate::admin_action_validation::validate;

#[test]
fn create_partitions_accepts_inclusive_bounds_and_reserves_identity() {
    let client_id = client("client-1");
    let clients = BTreeMap::from([(client_id.clone(), false)]);
    let mut operation_ids = BTreeSet::new();
    let mut problems = Vec::new();

    validate(
        &create_partitions(
            client_id.clone(),
            operation("admin-partitions-min"),
            "a".to_owned(),
            1,
            100,
        ),
        &clients,
        &mut operation_ids,
        &mut problems,
    );
    let maximum = operation("admin-partitions-max");
    validate(
        &create_partitions(client_id, maximum.clone(), "a".repeat(249), 10_000, 60_000),
        &clients,
        &mut operation_ids,
        &mut problems,
    );

    assert!(problems.is_empty(), "{problems:?}");
    assert!(operation_ids.contains(&maximum));
}

#[test]
fn create_partitions_requires_one_live_client() {
    let missing = client("missing-client");
    let shutdown = client("shutdown-client");
    let clients = BTreeMap::from([(shutdown.clone(), true)]);
    let mut operation_ids = BTreeSet::new();
    let mut problems = Vec::new();

    validate(
        &create_partitions(
            missing,
            operation("admin-missing"),
            "records".to_owned(),
            2,
            1_000,
        ),
        &clients,
        &mut operation_ids,
        &mut problems,
    );
    validate(
        &create_partitions(
            shutdown,
            operation("admin-shutdown"),
            "records".to_owned(),
            2,
            1_000,
        ),
        &clients,
        &mut operation_ids,
        &mut problems,
    );

    assert!(
        problems
            .iter()
            .any(|problem| problem.contains("missing client"))
    );
    assert!(
        problems
            .iter()
            .any(|problem| problem.contains("shut down client"))
    );
}

#[test]
fn create_partitions_rejects_duplicate_identity_and_out_of_range_fields() {
    let client_id = client("client-1");
    let duplicate = operation("admin-duplicate");
    let clients = BTreeMap::from([(client_id.clone(), false)]);
    let mut operation_ids = BTreeSet::from([duplicate.clone()]);
    let mut problems = Vec::new();

    validate(
        &create_partitions(client_id, duplicate, String::new(), 0, 99),
        &clients,
        &mut operation_ids,
        &mut problems,
    );
    validate(
        &create_partitions(
            client("client-1"),
            operation("admin-too-large"),
            "a".repeat(250),
            10_001,
            60_001,
        ),
        &clients,
        &mut operation_ids,
        &mut problems,
    );

    for expected in [
        "duplicate operation id",
        "invalid topic",
        "total_count must be between 1 and 10000",
        "timeout_ms must be between 100 and 60000",
    ] {
        assert!(
            problems.iter().any(|problem| problem.contains(expected)),
            "missing {expected:?} in {problems:?}"
        );
    }
}

#[test]
fn create_partitions_requires_the_admin_capability() {
    let client_id = client("client-1");
    let scenario = Scenario {
        schema_version: SCENARIO_SCHEMA_VERSION,
        id: scenario("admin.partitions-capability"),
        title: "partition capability".to_owned(),
        description: "partition administration requires an explicit capability".to_owned(),
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
                "create-partitions",
                create_partitions(
                    client_id.clone(),
                    operation("admin-partitions-1"),
                    "records".to_owned(),
                    2,
                    1_000,
                ),
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

    assert!(
        error
            .problems
            .iter()
            .any(|problem| problem == "admin steps require the admin capability")
    );
}

#[test]
fn create_partitions_shares_identity_space_with_topic_creation() {
    let client_id = client("client-1");
    let operation_id = operation("admin-shared");
    let clients = BTreeMap::from([(client_id.clone(), false)]);
    let mut operation_ids = BTreeSet::new();
    let mut problems = Vec::new();

    validate(
        &ScenarioAction::CreateTopic(CreateTopicAction {
            client_id: client_id.clone(),
            operation_id: operation_id.clone(),
            topic: "records".to_owned(),
            partitions: 1,
            replication_factor: 1,
            validate_only: false,
            expected_error_code: None,
            timeout_ms: 1_000,
        }),
        &clients,
        &mut operation_ids,
        &mut problems,
    );
    validate(
        &create_partitions(client_id, operation_id, "records".to_owned(), 2, 1_000),
        &clients,
        &mut operation_ids,
        &mut problems,
    );

    assert!(
        problems
            .iter()
            .any(|problem| problem.contains("duplicate operation id"))
    );
}

fn create_partitions(
    client_id: ClientId,
    operation_id: OperationId,
    topic: String,
    total_count: i32,
    timeout_ms: u64,
) -> ScenarioAction {
    ScenarioAction::CreatePartitions(CreatePartitionsAction {
        client_id,
        operation_id,
        topic,
        total_count,
        validate_only: false,
        expected_current_count: None,
        expected_error_code: None,
        timeout_ms,
    })
}

fn client(value: &str) -> ClientId {
    ClientId::new(value).unwrap_or_else(|error| panic!("client id: {error}"))
}

fn operation(value: &str) -> OperationId {
    OperationId::new(value).unwrap_or_else(|error| panic!("operation id: {error}"))
}

fn scenario(value: &str) -> ScenarioId {
    ScenarioId::new(value).unwrap_or_else(|error| panic!("scenario id: {error}"))
}

fn step(value: &str, action: ScenarioAction) -> ScenarioStep {
    ScenarioStep {
        id: StepId::new(value).unwrap_or_else(|error| panic!("step id: {error}")),
        action,
    }
}
