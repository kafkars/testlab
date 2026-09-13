//! Fetch-evidence fixtures pin the optional wire claim and independent prerequisites.

use crate::{
    AdapterEvent, AssignedConsumerFetchEvidence, Capability, OperationId, Scenario, ScenarioAction,
    TopicSelection,
};

#[test]
fn receive_event_round_trips_complete_fetch_evidence() {
    let event = AdapterEvent::ReceiveCompleted {
        receive_id: operation("receive-fetch"),
        records: Vec::new(),
        fetch_evidence: Some(AssignedConsumerFetchEvidence {
            topic: "records".to_owned(),
            topic_uuid: [7; 16],
            partition: 2,
            requested_offset: 3,
            next_offset: 5,
            checkpoint_next_offset: 5,
            log_start_offset: Some(1),
            last_stable_offset: Some(8),
            high_watermark: Some(8),
            retained_bytes: 256,
        }),
    };

    let encoded = serde_json::to_string(&event)
        .unwrap_or_else(|error| panic!("serialize Fetch evidence: {error}"));
    let decoded = serde_json::from_str::<AdapterEvent>(&encoded)
        .unwrap_or_else(|error| panic!("deserialize Fetch evidence: {error}"));

    assert_eq!(decoded, event);
    assert!(encoded.contains("\"checkpoint_next_offset\":5"));
    assert!(encoded.contains("\"retained_bytes\":256"));
}

#[test]
fn checked_in_fetch_evidence_scenario_is_valid() {
    scenario()
        .validate()
        .unwrap_or_else(|error| panic!("validate Fetch evidence: {error}"));
}

#[test]
fn selected_fetch_evidence_requires_its_capability() {
    let mut scenario = scenario();
    scenario
        .requires
        .remove(&Capability::AssignedConsumerFetchEvidence);
    assert_problem(&scenario, "assigned_consumer_fetch_evidence capability");
}

#[test]
fn selected_fetch_evidence_requires_prior_independent_topic_identity() {
    let mut scenario = scenario();
    let Some(ScenarioAction::DescribeTopics(action)) = scenario
        .steps
        .iter_mut()
        .find(|step| step.id.as_str() == "describe-topic-id")
        .map(|step| &mut step.action)
    else {
        panic!("topic-ID step missing");
    };
    action.selection = TopicSelection::Name;

    assert_problem(&scenario, "prior topic-ID description");
}

fn scenario() -> Scenario {
    toml::from_str(include_str!(
        "../../../scenarios/kafka/assigned-consumer-fetch-evidence.toml"
    ))
    .unwrap_or_else(|error| panic!("parse Fetch-evidence scenario: {error}"))
}

fn assert_problem(scenario: &Scenario, expected: &str) {
    let error = scenario.validation_error("Fetch-evidence fixture must be invalid");
    assert!(
        error
            .problems
            .iter()
            .any(|problem| problem.contains(expected)),
        "missing {expected:?} in {:?}",
        error.problems
    );
}

fn operation(value: &str) -> OperationId {
    OperationId::new(value).unwrap_or_else(|error| panic!("operation id: {error}"))
}
