//! Tests for the producer receipt validation contract.

use crate::{Capability, Scenario, ScenarioAction};

fn scenario() -> Scenario {
    toml::from_str(include_str!(
        "../../../scenarios/kafka/producer-receipt-metadata.toml"
    ))
    .unwrap_or_else(|error| panic!("parse producer receipt scenario: {error}"))
}

#[test]
fn checked_in_receipt_scenario_is_valid_and_capability_gated() {
    let mut scenario = scenario();
    scenario
        .validate()
        .unwrap_or_else(|error| panic!("validate producer receipt scenario: {error}"));
    assert!(
        scenario
            .requires
            .remove(&Capability::ProducerReceiptMetadata)
    );
    assert!(
        scenario
            .validation_error("receipt capability must be explicit")
            .to_string()
            .contains("producer_receipt_metadata")
    );
}

#[test]
fn receipt_send_requires_prior_exact_successful_topology() {
    let mut scenario = scenario();
    let description = scenario
        .steps
        .iter_mut()
        .find_map(|step| match &mut step.action {
            ScenarioAction::DescribeTopics(action) => Some(action),
            _ => None,
        })
        .unwrap_or_else(|| panic!("topic-ID description missing"));
    description.topics[0].expected_partitions = Some(vec![1]);
    let error = scenario
        .validation_error("mismatched topology must fail")
        .to_string();
    assert!(error.contains("requires successful exact topic-ID topology"));
}
