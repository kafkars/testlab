//! Batched record deletion schemas pin boundaries, caller order, and baselines.

use std::collections::{BTreeMap, BTreeSet};

use crate::{
    AdapterCommand, AdapterEvent, AdminRecordsBatchDeleted, AdminRecordsDeletionOutcome, ClientId,
    DeleteRecordsBatchAction, DeleteRecordsBatchCommand, DeleteRecordsBatchExpectation,
    DeleteRecordsBatchSelection, DeleteRecordsBoundary, EVIDENCE_SCHEMA_VERSION, OperationId,
    PROTOCOL_VERSION, SCENARIO_SCHEMA_VERSION, Scenario, ScenarioAction,
};

#[test]
fn record_deletion_batch_advances_every_versioned_boundary() {
    assert_eq!(PROTOCOL_VERSION, 139);
    assert_eq!(SCENARIO_SCHEMA_VERSION, 143);
    assert_eq!(EVIDENCE_SCHEMA_VERSION, 129);
}

#[test]
fn action_command_and_completion_round_trip_without_leaking_baselines() {
    round_trip(&ScenarioAction::DeleteRecordsBatch(action()));
    round_trip(&AdapterCommand::DeleteRecordsBatch(command()));
    round_trip(&AdapterEvent::RecordsBatchDeleted(completion()));
    let encoded = toml::to_string(&command())
        .unwrap_or_else(|error| panic!("encode record-deletion batch command: {error}"));
    assert!(!encoded.contains("expected_high_watermark"));
    assert!(encoded.contains("high_watermark"));
}

#[test]
fn checked_in_batch_scenario_is_valid() {
    scenario()
        .validate()
        .unwrap_or_else(|error| panic!("validate record-deletion batch scenario: {error}"));
}

#[test]
fn validation_rejects_nonplural_and_duplicate_targets() {
    for (label, targets, expected) in [
        (
            "singleton",
            vec![expectations()[0].clone()],
            "2 to 32 entries",
        ),
        (
            "duplicate",
            vec![expectations()[0].clone(), expectations()[0].clone()],
            "targets must be unique",
        ),
    ] {
        let mut value = action();
        value.operation_id = operation(label);
        value.targets = targets;
        let mut problems = Vec::new();
        crate::admin_action_validation::validate(
            &ScenarioAction::DeleteRecordsBatch(value),
            &BTreeMap::from([(client(), false)]),
            &mut BTreeSet::new(),
            &mut problems,
        );
        assert!(
            problems.iter().any(|problem| problem.contains(expected)),
            "{label}: {problems:?}"
        );
    }
}

#[test]
fn transition_requires_complete_baselines_for_every_target() {
    let mut value = scenario();
    value
        .steps
        .retain(|step| step.id.as_str() != "list-batch-baseline-latest");
    let error = value
        .validate()
        .expect_err("batch deletion without latest baselines");
    assert!(
        error
            .to_string()
            .contains("requires same-target earliest offset 0 followed by latest offset"),
        "{error}"
    );
}

fn action() -> DeleteRecordsBatchAction {
    DeleteRecordsBatchAction {
        client_id: client(),
        operation_id: operation("delete-records-batch"),
        targets: expectations(),
        timeout_ms: 1_000,
    }
}

fn command() -> DeleteRecordsBatchCommand {
    DeleteRecordsBatchCommand {
        client_id: client(),
        operation_id: operation("delete-records-batch"),
        targets: selections(),
        timeout_ms: 1_000,
    }
}

fn completion() -> AdminRecordsBatchDeleted {
    AdminRecordsBatchDeleted {
        operation_id: operation("delete-records-batch"),
        outcomes: vec![
            AdminRecordsDeletionOutcome {
                topic: "records".to_owned(),
                partition: 1,
                low_watermark: Some(2),
                error_code: None,
            },
            AdminRecordsDeletionOutcome {
                topic: "records".to_owned(),
                partition: 0,
                low_watermark: Some(3),
                error_code: None,
            },
        ],
    }
}

fn expectations() -> Vec<DeleteRecordsBatchExpectation> {
    vec![
        DeleteRecordsBatchExpectation {
            topic: "records".to_owned(),
            partition: 1,
            boundary: DeleteRecordsBoundary::Offset { offset: 2 },
            expected_high_watermark: 3,
        },
        DeleteRecordsBatchExpectation {
            topic: "records".to_owned(),
            partition: 0,
            boundary: DeleteRecordsBoundary::HighWatermark,
            expected_high_watermark: 3,
        },
    ]
}

fn selections() -> Vec<DeleteRecordsBatchSelection> {
    expectations()
        .into_iter()
        .map(|target| DeleteRecordsBatchSelection {
            topic: target.topic,
            partition: target.partition,
            boundary: target.boundary,
        })
        .collect()
}

fn scenario() -> Scenario {
    toml::from_str(include_str!(
        "../../../scenarios/kafka/admin-delete-records-batch.toml"
    ))
    .unwrap_or_else(|error| panic!("parse record-deletion batch scenario: {error}"))
}

fn round_trip<T>(value: &T)
where
    T: serde::Serialize + serde::de::DeserializeOwned + std::fmt::Debug + PartialEq,
{
    let encoded = serde_json::to_string(value)
        .unwrap_or_else(|error| panic!("encode record-deletion batch: {error}"));
    let decoded = serde_json::from_str::<T>(&encoded)
        .unwrap_or_else(|error| panic!("decode record-deletion batch: {error}"));
    assert_eq!(&decoded, value);
}

fn client() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client: {error}"))
}

fn operation(value: &str) -> OperationId {
    OperationId::new(value).unwrap_or_else(|error| panic!("operation: {error}"))
}
