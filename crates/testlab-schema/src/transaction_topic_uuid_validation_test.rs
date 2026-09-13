//! Tests for the transaction topic uuid validation contract.

use crate::{Capability, Scenario, ScenarioAction, TransactionDisposition};

fn scenario() -> Scenario {
    toml::from_str(include_str!(
        "../../../scenarios/kafka/transaction-topic-uuid-commit.toml"
    ))
    .unwrap_or_else(|error| panic!("parse UUID-bound transaction scenario: {error}"))
}

#[test]
fn checked_in_uuid_bound_transaction_is_valid_and_capability_gated() {
    let mut scenario = scenario();
    scenario
        .validate()
        .unwrap_or_else(|error| panic!("validate UUID-bound transaction: {error}"));
    assert!(
        scenario
            .requires
            .remove(&Capability::TransactionTopicUuidValidation)
    );
    assert!(
        scenario
            .validation_error("UUID-bound capability must be explicit")
            .to_string()
            .contains("transaction_topic_uuid_validation")
    );
}

#[test]
fn uuid_bound_transaction_requires_commit_and_exact_topics() {
    let mut scenario = scenario();
    let transaction = scenario
        .steps
        .iter_mut()
        .find_map(|step| match &mut step.action {
            ScenarioAction::ExecuteTransaction {
                disposition,
                operations,
                ..
            } => Some((disposition, operations)),
            _ => None,
        })
        .unwrap_or_else(|| panic!("UUID-bound transaction missing"));
    *transaction.0 = TransactionDisposition::Abort;
    transaction.1[0].record.topic = "different-topic".to_owned();
    let error = scenario
        .validation_error("abort and mismatched topics must fail")
        .to_string();
    assert!(error.contains("must commit after validation"));
    assert!(error.contains("exactly order its distinct record topics"));
}
