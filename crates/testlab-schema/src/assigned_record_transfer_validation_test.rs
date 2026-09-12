//! Owned record-transfer fixtures pin capability, identity, and reserved metadata.

use crate::{AssignedRecordConversionMethod, Capability, HeaderSpec, Scenario, ScenarioAction};

#[test]
fn checked_in_owned_transfer_is_valid_and_exact() {
    let scenario = scenario();
    scenario
        .validate()
        .unwrap_or_else(|error| panic!("validate owned transfer: {error}"));
    let Some(ScenarioAction::TransferAssignedRecord(action)) = scenario
        .steps
        .iter()
        .find(|step| step.id.as_str() == "transfer-owned-record")
        .map(|step| &step.action)
    else {
        panic!("owned transfer action missing");
    };
    assert_eq!(
        action.expected_input_operation_id.as_str(),
        "owned-transfer-source"
    );
    assert_eq!(action.operation_id.as_str(), "owned-transfer-destination");
    assert_eq!(
        action.method,
        AssignedRecordConversionMethod::IntoOwnedBatch
    );
    assert_eq!(action.target_partition, 1);
}

#[test]
fn checked_in_direct_owned_records_transfer_is_valid_and_exact() {
    let scenario = direct_scenario();
    scenario
        .validate()
        .unwrap_or_else(|error| panic!("validate direct owned-record transfer: {error}"));
    let Some(ScenarioAction::TransferAssignedRecord(action)) = scenario
        .steps
        .iter()
        .find(|step| step.id.as_str() == "transfer-owned-records-directly")
        .map(|step| &step.action)
    else {
        panic!("direct owned-record transfer action missing");
    };
    assert_eq!(
        action.method,
        AssignedRecordConversionMethod::IntoOwnedRecords
    );
}

#[test]
fn owned_transfer_requires_its_exact_capability() {
    let mut scenario = scenario();
    scenario
        .requires
        .remove(&Capability::AssignedConsumerRecordTransfer);
    assert_problem(&scenario, "assigned_consumer_record_transfer capability");
}

#[test]
fn source_cannot_forge_reserved_transfer_correlation() {
    let mut scenario = scenario();
    let Some(ScenarioAction::Send { record, .. }) = scenario
        .steps
        .iter_mut()
        .find(|step| step.id.as_str() == "send-source-record")
        .map(|step| &mut step.action)
    else {
        panic!("source send missing");
    };
    record.headers.push(HeaderSpec {
        name: crate::RECORD_TRANSFER_OPERATION_HEADER.to_owned(),
        value: None,
    });
    assert_problem(&scenario, "already contains reserved header");
}

#[test]
fn source_must_be_an_explicit_record_operation() {
    let mut scenario = scenario();
    let Some(ScenarioAction::TransferAssignedRecord(action)) = scenario
        .steps
        .iter_mut()
        .find(|step| step.id.as_str() == "transfer-owned-record")
        .map(|step| &mut step.action)
    else {
        panic!("owned transfer action missing");
    };
    action.expected_input_operation_id = crate::OperationId::new("missing-source")
        .unwrap_or_else(|error| panic!("source operation id: {error}"));
    assert_problem(&scenario, "references unsupported source operation");
}

fn scenario() -> Scenario {
    toml::from_str(include_str!(
        "../../../scenarios/kafka/assigned-consumer-owned-record-transfer.toml"
    ))
    .unwrap_or_else(|error| panic!("parse owned transfer: {error}"))
}

fn direct_scenario() -> Scenario {
    toml::from_str(include_str!(
        "../../../scenarios/kafka/assigned-consumer-direct-owned-records-transfer.toml"
    ))
    .unwrap_or_else(|error| panic!("parse direct owned-record transfer: {error}"))
}

fn assert_problem(scenario: &Scenario, expected: &str) {
    let error = scenario
        .validate()
        .expect_err("owned transfer fixture must be invalid");
    assert!(
        error
            .problems
            .iter()
            .any(|problem| problem.contains(expected)),
        "missing {expected:?} in {:?}",
        error.problems
    );
}
