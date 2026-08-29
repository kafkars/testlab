//! Delete-records schema tests pin expectation ownership and ordered preconditions.

use std::collections::{BTreeMap, BTreeSet};

use super::{
    AdapterCommand, AdapterEvent, AdminOffsetPosition, AdminRecordsDeleted, BrokerPartitionOffsets,
    BrokerStateObservation, Capability, ClientId, DeleteRecordsAction, DeleteRecordsCommand,
    EVIDENCE_SCHEMA_VERSION, ListOffsetsAction, OperationId, PROTOCOL_VERSION,
    SCENARIO_SCHEMA_VERSION, Scenario, ScenarioAction, ScenarioId, ScenarioStep, StepId,
};

#[test]
fn delete_records_versions_and_wire_facts_are_exact() {
    assert_eq!(PROTOCOL_VERSION, 34);
    assert_eq!(SCENARIO_SCHEMA_VERSION, 37);
    assert_eq!(EVIDENCE_SCHEMA_VERSION, 26);

    let action = ScenarioAction::DeleteRecords(delete_action());
    let command = AdapterCommand::DeleteRecords(delete_command());
    let event = AdapterEvent::RecordsDeleted(AdminRecordsDeleted {
        operation_id: operation("admin-delete-records"),
        topic: "records".to_owned(),
        partition: 0,
        low_watermark: 2,
    });
    let observation = BrokerStateObservation::PartitionOffsets(BrokerPartitionOffsets {
        observation: 4,
        operation_id: operation("admin-delete-records"),
        topic: "records".to_owned(),
        partition: 0,
        low_watermark: 2,
        high_watermark: 3,
    });

    let action_encoded = encode(&action);
    let command_encoded = encode(&command);
    let event_encoded = encode(&event);
    let observation_encoded = encode(&observation);
    assert!(action_encoded.contains("kind = \"delete_records\""));
    assert!(action_encoded.contains("expected_high_watermark = 3"));
    assert!(!command_encoded.contains("expected_high_watermark"));
    assert!(event_encoded.contains("kind = \"records_deleted\""));
    assert!(event_encoded.contains("low_watermark = 2"));
    assert!(observation_encoded.contains("kind = \"partition_offsets\""));
    assert_eq!(decode::<ScenarioAction>(&action_encoded), action);
    assert_eq!(decode::<AdapterCommand>(&command_encoded), command);
    assert_eq!(decode::<AdapterEvent>(&event_encoded), event);
    assert_eq!(
        decode::<BrokerStateObservation>(&observation_encoded),
        observation
    );
}

#[test]
fn delete_records_command_rejects_scenario_expectations() {
    let encoded = encode(&AdapterCommand::DeleteRecords(delete_command()));
    let injected = format!("{encoded}expected_high_watermark = 3\n");

    assert!(toml::from_str::<AdapterCommand>(&injected).is_err());
}

#[test]
fn delete_records_validation_reuses_admin_ownership_and_bounds() {
    let duplicate = operation("admin-delete-records");
    let clients = BTreeMap::from([(client("client-1"), false)]);
    let mut operations = BTreeSet::new();
    let mut problems = Vec::new();
    let invalid = ScenarioAction::DeleteRecords(DeleteRecordsAction {
        client_id: client("client-1"),
        operation_id: duplicate.clone(),
        topic: String::new(),
        partition: -1,
        before_offset: 0,
        expected_high_watermark: 0,
        timeout_ms: 0,
    });
    let missing = ScenarioAction::DeleteRecords(DeleteRecordsAction {
        client_id: client("missing-client"),
        operation_id: duplicate,
        topic: "records".to_owned(),
        partition: 0,
        before_offset: 1,
        expected_high_watermark: 2,
        timeout_ms: 100,
    });

    crate::admin_action_validation::validate(&invalid, &clients, &mut operations, &mut problems);
    crate::admin_action_validation::validate(&missing, &clients, &mut operations, &mut problems);

    for expected in [
        "has invalid topic",
        "partition must be nonnegative",
        "before_offset must be positive",
        "expected_high_watermark must exceed before_offset",
        "timeout_ms must be between 100 and 60000",
        "uses missing client",
        "duplicate operation id",
    ] {
        assert_problem(&problems, expected);
    }
    let mut usage = BTreeSet::new();
    crate::scenario_capability_validation::record_usage(&invalid, &mut usage);
    assert_eq!(usage, BTreeSet::from([Capability::Admin]));
}

#[test]
fn ordered_same_target_watermark_baseline_is_required() {
    valid_scenario()
        .validate()
        .unwrap_or_else(|error| panic!("valid delete-records scenario: {error}"));

    let mut reversed = valid_scenario();
    reversed.steps.swap(1, 2);
    assert_invalid_transition(&reversed);

    let mut mismatched = valid_scenario();
    let ScenarioAction::DeleteRecords(action) = &mut mismatched.steps[3].action else {
        panic!("delete-records step changed shape");
    };
    action.expected_high_watermark = 4;
    assert_invalid_transition(&mismatched);

    let mut wrong_target = valid_scenario();
    let ScenarioAction::ListOffsets(action) = &mut wrong_target.steps[2].action else {
        panic!("latest-offset step changed shape");
    };
    action.partition = 1;
    assert_invalid_transition(&wrong_target);
}

#[test]
fn delete_records_scenarios_require_admin_capability() {
    let mut scenario = valid_scenario();
    scenario.requires.remove(&Capability::Admin);

    let error = match scenario.validate() {
        Ok(()) => panic!("delete-records without admin capability must fail"),
        Err(error) => error,
    };
    assert_problem(&error.problems, "admin steps require the admin capability");
}

fn valid_scenario() -> Scenario {
    let client_id = client("client-1");
    Scenario {
        schema_version: SCENARIO_SCHEMA_VERSION,
        id: ScenarioId::new("admin.delete-records")
            .unwrap_or_else(|error| panic!("scenario id: {error}")),
        title: "delete records".to_owned(),
        description: "delete a bounded partition prefix".to_owned(),
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
                "list-earliest",
                list_offsets(
                    client_id.clone(),
                    "admin-earliest",
                    AdminOffsetPosition::Earliest,
                    0,
                ),
            ),
            step(
                "list-latest",
                list_offsets(
                    client_id.clone(),
                    "admin-latest",
                    AdminOffsetPosition::Latest,
                    3,
                ),
            ),
            step(
                "delete-records",
                ScenarioAction::DeleteRecords(DeleteRecordsAction {
                    client_id: client_id.clone(),
                    operation_id: operation("admin-delete-records"),
                    topic: "records".to_owned(),
                    partition: 0,
                    before_offset: 2,
                    expected_high_watermark: 3,
                    timeout_ms: 1_000,
                }),
            ),
            step(
                "shutdown-client",
                ScenarioAction::ShutdownClient { client_id },
            ),
        ],
        assertions: Vec::new(),
    }
}

fn list_offsets(
    client_id: ClientId,
    operation_id: &str,
    position: AdminOffsetPosition,
    expected_offset: i64,
) -> ScenarioAction {
    ScenarioAction::ListOffsets(ListOffsetsAction {
        client_id,
        operation_id: operation(operation_id),
        topic: "records".to_owned(),
        partition: 0,
        position,
        expected_offset: Some(expected_offset),
        expected_error_code: None,
        timeout_ms: 1_000,
    })
}

fn delete_action() -> DeleteRecordsAction {
    DeleteRecordsAction {
        client_id: client("client-1"),
        operation_id: operation("admin-delete-records"),
        topic: "records".to_owned(),
        partition: 0,
        before_offset: 2,
        expected_high_watermark: 3,
        timeout_ms: 1_000,
    }
}

fn delete_command() -> DeleteRecordsCommand {
    DeleteRecordsCommand {
        client_id: client("client-1"),
        operation_id: operation("admin-delete-records"),
        topic: "records".to_owned(),
        partition: 0,
        before_offset: 2,
        timeout_ms: 1_000,
    }
}

fn assert_invalid_transition(scenario: &Scenario) {
    let error = match scenario.validate() {
        Ok(()) => panic!("invalid delete-records transition must fail"),
        Err(error) => error,
    };
    assert_problem(
        &error.problems,
        "requires same-target earliest offset 0 followed by latest offset",
    );
}

fn assert_problem(problems: &[String], expected: &str) {
    assert!(
        problems.iter().any(|problem| problem.contains(expected)),
        "missing {expected:?} in {problems:?}"
    );
}

fn encode<T: serde::Serialize>(value: &T) -> String {
    toml::to_string(value).unwrap_or_else(|error| panic!("serialize delete records: {error}"))
}

fn decode<T: serde::de::DeserializeOwned>(value: &str) -> T {
    toml::from_str(value).unwrap_or_else(|error| panic!("deserialize delete records: {error}"))
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
