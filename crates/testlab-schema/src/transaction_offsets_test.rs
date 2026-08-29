//! Transactional offset tests pin validation and the harness-only expectation boundary.

use crate::{AdapterCommand, Scenario, ScenarioAction};

fn classic() -> Scenario {
    toml::from_str(include_str!(
        "../../../scenarios/kafka/transactional-offset-classic.toml"
    ))
    .unwrap_or_else(|error| panic!("parse classic transactional offset scenario: {error}"))
}

#[test]
fn checked_in_transactional_offset_scenarios_are_valid() {
    for source in [
        include_str!("../../../scenarios/kafka/transactional-offset-classic.toml"),
        include_str!("../../../scenarios/kafka/transactional-offset-consumer.toml"),
    ] {
        let scenario: Scenario =
            toml::from_str(source).unwrap_or_else(|error| panic!("parse scenario: {error}"));
        scenario
            .validate()
            .unwrap_or_else(|error| panic!("validate scenario: {error}"));
    }
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
