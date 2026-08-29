//! Batched topic-creation tests pin ordered wire facts and scenario preconditions.

use std::collections::{BTreeMap, BTreeSet};

use super::{
    AdapterCommand, AdapterEvent, AdminTopicCreationOutcome, AdminTopicsCreationBatch, Capability,
    ClientId, CreateTopicAction, CreateTopicBatchActionItem, CreateTopicBatchCommandItem,
    CreateTopicsBatchAction, CreateTopicsBatchCommand, EVIDENCE_SCHEMA_VERSION, OperationId,
    PROTOCOL_VERSION, SCENARIO_SCHEMA_VERSION, Scenario, ScenarioAction, ScenarioId, ScenarioStep,
    StepId, TOPIC_ALREADY_EXISTS_ERROR_CODE,
};

#[test]
fn batch_versions_and_ordered_wire_facts_are_exact() {
    assert_eq!(PROTOCOL_VERSION, 34);
    assert_eq!(SCENARIO_SCHEMA_VERSION, 37);
    assert_eq!(EVIDENCE_SCHEMA_VERSION, 26);

    let action = ScenarioAction::CreateTopicsBatch(batch_action());
    let command = AdapterCommand::CreateTopicsBatch(batch_command());
    let event = AdapterEvent::TopicsCreationCompleted(AdminTopicsCreationBatch {
        operation_id: operation("admin-create-topics-batch"),
        outcomes: vec![
            outcome("new-topic", None),
            outcome("existing-topic", Some(TOPIC_ALREADY_EXISTS_ERROR_CODE)),
        ],
    });

    let action_encoded = encode(&action);
    let command_encoded = encode(&command);
    let event_encoded = encode(&event);
    assert!(action_encoded.contains("kind = \"create_topics_batch\""));
    assert!(action_encoded.contains("expected_error_code"));
    assert!(!command_encoded.contains("expected_error_code"));
    assert!(event_encoded.contains("kind = \"topics_creation_completed\""));
    assert_eq!(decode::<ScenarioAction>(&action_encoded), action);
    assert_eq!(decode::<AdapterCommand>(&command_encoded), command);
    assert_eq!(decode::<AdapterEvent>(&event_encoded), event);
}

#[test]
fn batch_command_rejects_scenario_expectations() {
    let encoded = encode(&AdapterCommand::CreateTopicsBatch(batch_command()));
    let injected =
        format!("{encoded}expected_error_code = \"{TOPIC_ALREADY_EXISTS_ERROR_CODE}\"\n");

    assert!(toml::from_str::<AdapterCommand>(&injected).is_err());
}

#[test]
fn batch_validation_reuses_admin_ownership_bounds_and_capability() {
    let operation_id = operation("admin-invalid-batch");
    let clients = BTreeMap::from([(client("client-1"), false)]);
    let mut operation_ids = BTreeSet::from([operation_id.clone()]);
    let mut problems = Vec::new();
    let invalid = ScenarioAction::CreateTopicsBatch(CreateTopicsBatchAction {
        client_id: client("missing-client"),
        operation_id,
        topics: vec![item("", 0, 0, Some("broker"))],
        timeout_ms: 99,
    });

    crate::admin_action_validation::validate(&invalid, &clients, &mut operation_ids, &mut problems);
    for expected in [
        "uses missing client",
        "duplicate operation id",
        "topics must contain 2 to 32 entries",
        "invalid batch topic",
        "partitions must be between 1 and 10000",
        "replication_factor must be between 1 and 100",
        TOPIC_ALREADY_EXISTS_ERROR_CODE,
        "timeout_ms must be between 100 and 60000",
    ] {
        assert_problem(&problems, expected);
    }

    let mut scenario = valid_mixed_scenario();
    scenario.requires.remove(&Capability::Admin);
    assert_problem(
        &scenario_problems(&scenario),
        "admin steps require the admin capability",
    );
}

#[test]
fn batch_topics_must_be_unique() {
    let clients = BTreeMap::from([(client("client-1"), false)]);
    let mut operation_ids = BTreeSet::new();
    let mut problems = Vec::new();
    let action = ScenarioAction::CreateTopicsBatch(CreateTopicsBatchAction {
        client_id: client("client-1"),
        operation_id: operation("admin-duplicate-resources"),
        topics: vec![item("same", 1, 1, None), item("same", 1, 1, None)],
        timeout_ms: 1_000,
    });

    crate::admin_action_validation::validate(&action, &clients, &mut operation_ids, &mut problems);
    assert_problem(&problems, "batch topics must be unique");
}

#[test]
fn mixed_outcomes_require_an_exact_preceding_singleton() {
    valid_mixed_scenario()
        .validate()
        .unwrap_or_else(|error| panic!("valid mixed batch: {error}"));

    let mut absent = valid_mixed_scenario();
    absent.steps.remove(1);
    assert_problem(
        &scenario_problems(&absent),
        "requires a prior identical successful singleton topic creation",
    );

    let mut mismatched = valid_mixed_scenario();
    let ScenarioAction::CreateTopic(action) = &mut mismatched.steps[1].action else {
        panic!("singleton step changed shape");
    };
    action.partitions = 1;
    assert_problem(
        &scenario_problems(&mismatched),
        "requires a prior identical successful singleton topic creation",
    );

    let mut prior_batch = valid_mixed_scenario();
    prior_batch.steps[1].action = ScenarioAction::CreateTopicsBatch(CreateTopicsBatchAction {
        client_id: client("client-1"),
        operation_id: operation("admin-prior-batch"),
        topics: vec![
            item("existing-topic", 2, 1, None),
            item("prior-batch-peer", 1, 1, None),
        ],
        timeout_ms: 1_000,
    });
    assert_problem(
        &scenario_problems(&prior_batch),
        "requires a prior identical successful singleton topic creation",
    );
}

#[test]
fn expected_success_rejects_a_topic_already_created() {
    let mut scenario = valid_mixed_scenario();
    let ScenarioAction::CreateTopicsBatch(action) = &mut scenario.steps[2].action else {
        panic!("batch step changed shape");
    };
    action.topics[1].expected_error_code = None;

    assert_problem(
        &scenario_problems(&scenario),
        "expects successful creation of existing topic existing-topic",
    );
}

fn valid_mixed_scenario() -> Scenario {
    let client_id = client("client-1");
    Scenario {
        schema_version: SCENARIO_SCHEMA_VERSION,
        id: ScenarioId::new("admin.create-topics-batch")
            .unwrap_or_else(|error| panic!("scenario id: {error}")),
        title: "batch topic creation".to_owned(),
        description: "ordered mixed per-resource results".to_owned(),
        timeout_ms: 10_000,
        requires: BTreeSet::from([Capability::Admin, Capability::Lifecycle]),
        steps: vec![
            step(
                "create-client",
                ScenarioAction::CreateClient {
                    client_id: client_id.clone(),
                },
            ),
            step(
                "create-existing",
                ScenarioAction::CreateTopic(CreateTopicAction {
                    client_id: client_id.clone(),
                    operation_id: operation("admin-create-existing"),
                    topic: "existing-topic".to_owned(),
                    partitions: 2,
                    replication_factor: 1,
                    validate_only: false,
                    expected_error_code: None,
                    timeout_ms: 1_000,
                }),
            ),
            step(
                "create-batch",
                ScenarioAction::CreateTopicsBatch(batch_action()),
            ),
            step(
                "shutdown-client",
                ScenarioAction::ShutdownClient { client_id },
            ),
        ],
        assertions: Vec::new(),
    }
}

fn batch_action() -> CreateTopicsBatchAction {
    CreateTopicsBatchAction {
        client_id: client("client-1"),
        operation_id: operation("admin-create-topics-batch"),
        topics: vec![
            item("new-topic", 1, 1, None),
            item(
                "existing-topic",
                2,
                1,
                Some(TOPIC_ALREADY_EXISTS_ERROR_CODE),
            ),
        ],
        timeout_ms: 1_000,
    }
}

fn batch_command() -> CreateTopicsBatchCommand {
    CreateTopicsBatchCommand {
        client_id: client("client-1"),
        operation_id: operation("admin-create-topics-batch"),
        topics: vec![
            command_item("new-topic", 1),
            command_item("existing-topic", 2),
        ],
        timeout_ms: 1_000,
    }
}

fn item(
    topic: &str,
    partitions: i32,
    replication_factor: i16,
    expected_error_code: Option<&str>,
) -> CreateTopicBatchActionItem {
    CreateTopicBatchActionItem {
        topic: topic.to_owned(),
        partitions,
        replication_factor,
        expected_error_code: expected_error_code.map(str::to_owned),
    }
}

fn command_item(topic: &str, partitions: i32) -> CreateTopicBatchCommandItem {
    CreateTopicBatchCommandItem {
        topic: topic.to_owned(),
        partitions,
        replication_factor: 1,
    }
}

fn outcome(topic: &str, error_code: Option<&str>) -> AdminTopicCreationOutcome {
    AdminTopicCreationOutcome {
        topic: topic.to_owned(),
        error_code: error_code.map(str::to_owned),
    }
}

fn scenario_problems(scenario: &Scenario) -> Vec<String> {
    match scenario.validate() {
        Ok(()) => panic!("scenario must fail"),
        Err(error) => error.problems,
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

fn encode<T: serde::Serialize>(value: &T) -> String {
    toml::to_string(value).unwrap_or_else(|error| panic!("serialize batch topic creation: {error}"))
}

fn decode<T: serde::de::DeserializeOwned>(value: &str) -> T {
    toml::from_str(value)
        .unwrap_or_else(|error| panic!("deserialize batch topic creation: {error}"))
}
