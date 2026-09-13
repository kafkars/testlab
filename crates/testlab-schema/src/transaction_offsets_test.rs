//! Transactional offset tests pin validation and the harness-only expectation boundary.

use crate::{AdapterCommand, Capability, Scenario, ScenarioAction, TransactionSendMethod};

fn classic() -> Scenario {
    toml::from_str(include_str!(
        "../../../scenarios/kafka/transactional-offset-classic.toml"
    ))
    .unwrap_or_else(|error| panic!("parse classic transactional offset scenario: {error}"))
}

fn batch() -> Scenario {
    toml::from_str(include_str!(
        "../../../scenarios/kafka/transaction-batch-commit.toml"
    ))
    .unwrap_or_else(|error| panic!("parse transaction batch scenario: {error}"))
}

#[test]
fn checked_in_transactional_offset_scenarios_are_valid() {
    for source in [
        include_str!("../../../scenarios/kafka/transactional-offset-classic.toml"),
        include_str!("../../../scenarios/kafka/transactional-offset-consumer.toml"),
        include_str!(
            "../../../scenarios/kafka/classic-transactional-transform-network-connection-cut-recovery.toml"
        ),
        include_str!(
            "../../../scenarios/kafka/consumer-transactional-transform-network-connection-cut-recovery.toml"
        ),
    ] {
        let scenario: Scenario =
            toml::from_str(source).unwrap_or_else(|error| panic!("parse scenario: {error}"));
        scenario
            .validate()
            .unwrap_or_else(|error| panic!("validate scenario: {error}"));
    }
}

#[test]
fn checked_in_transaction_batch_scenarios_are_valid() {
    for source in [
        include_str!("../../../scenarios/kafka/transaction-batch-commit.toml"),
        include_str!("../../../scenarios/kafka/transaction-batch-abort.toml"),
        include_str!("../../../scenarios/kafka/transaction-partition-leader-failover.toml"),
    ] {
        let scenario: Scenario =
            toml::from_str(source).unwrap_or_else(|error| panic!("parse scenario: {error}"));
        scenario
            .validate()
            .unwrap_or_else(|error| panic!("validate scenario: {error}"));
    }
}

#[test]
fn transactional_batch_requires_its_capability_and_homogeneous_records() {
    let mut scenario = batch();
    assert!(scenario.requires.remove(&Capability::TransactionBatchSend));
    let error = scenario
        .validate()
        .expect_err("batch capability must be explicit");
    assert!(
        error
            .problems
            .iter()
            .any(|problem| problem.contains("transaction_batch_send capability"))
    );

    let mut scenario = batch();
    let action = scenario
        .steps
        .iter_mut()
        .find_map(|step| match &mut step.action {
            ScenarioAction::ExecuteTransaction {
                method: TransactionSendMethod::SendBatch,
                operations,
                ..
            } => Some(operations),
            _ => None,
        })
        .unwrap_or_else(|| panic!("transactional batch action missing"));
    action[1].record.partition = 1;
    let error = scenario
        .validate()
        .expect_err("heterogeneous batch must fail");
    assert!(
        error
            .problems
            .iter()
            .any(|problem| problem.contains("share one topic and partition"))
    );
}

#[test]
fn transactional_transform_requires_output_records() {
    let mut scenario = classic();
    let Some(ScenarioAction::ExecuteTransactionalTransform(action)) =
        scenario.steps.get_mut(6).map(|step| &mut step.action)
    else {
        panic!("commit transform missing");
    };
    action.operations.clear();

    let error = match scenario.validate() {
        Ok(()) => panic!("empty transform outputs must fail"),
        Err(error) => error,
    };
    assert!(
        error
            .problems
            .iter()
            .any(|problem| problem.contains("between 1 and 31 output records"))
    );
}

#[test]
fn expected_input_identity_does_not_cross_the_adapter_boundary() {
    let scenario = classic();
    let Some(ScenarioAction::ExecuteTransactionalTransform(action)) =
        scenario.steps.get(6).map(|step| &step.action)
    else {
        panic!("commit transform missing");
    };
    let command =
        AdapterCommand::ExecuteTransactionalTransform(crate::TransactionalTransformCommand {
            producer_id: action.producer_id.clone(),
            consumer_id: action.consumer_id.clone(),
            transaction_id: action.transaction_id.clone(),
            operations: action.operations.clone(),
            disposition: action.disposition,
            timeout_ms: action.timeout_ms,
        });
    let json = serde_json::to_string(&command)
        .unwrap_or_else(|error| panic!("serialize transform command: {error}"));
    assert!(!json.contains("expected_input_operation_id"));
}
