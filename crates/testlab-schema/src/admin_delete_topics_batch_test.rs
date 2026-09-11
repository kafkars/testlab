//! Plural topic-deletion schemas pin caller order, mixed outcomes, and prior state.

use std::collections::{BTreeMap, BTreeSet};

use super::{
    AdapterCommand, AdapterEvent, AdminTopicDeletionOutcome, AdminTopicsDeletion, ClientId,
    DeleteTopicExpectation, DeleteTopicsAction, DeleteTopicsCommand, EVIDENCE_SCHEMA_VERSION,
    OperationId, PROTOCOL_VERSION, SCENARIO_SCHEMA_VERSION, Scenario, ScenarioAction,
    UNKNOWN_TOPIC_OR_PARTITION_ERROR_CODE,
};

#[test]
fn plural_topic_deletion_advances_all_versioned_boundaries() {
    assert_eq!(PROTOCOL_VERSION, 56);
    assert_eq!(SCENARIO_SCHEMA_VERSION, 59);
    assert_eq!(EVIDENCE_SCHEMA_VERSION, 45);
}

#[test]
fn action_command_and_completion_round_trip_in_caller_order() {
    round_trip(&ScenarioAction::DeleteTopics(action()));
    round_trip(&AdapterCommand::DeleteTopics(command()));
    round_trip(&AdapterEvent::TopicsDeleted(completion()));
    assert_eq!(command().topics, topic_names());
}

#[test]
fn checked_in_plural_topic_deletion_scenario_is_valid() {
    scenario()
        .validate()
        .unwrap_or_else(|error| panic!("validate plural topic deletion: {error}"));
}

#[test]
fn validation_rejects_nonplural_duplicate_and_wrong_error() {
    for (label, topics, expected) in [
        (
            "singleton",
            vec![expectation("topic-a", None)],
            "between 2 and 32 entries",
        ),
        (
            "duplicate",
            vec![expectation("topic-a", None), expectation("topic-a", None)],
            "unique valid names",
        ),
        (
            "error",
            vec![
                expectation("topic-a", None),
                expectation("topic-b", Some("broker:broker_29")),
            ],
            UNKNOWN_TOPIC_OR_PARTITION_ERROR_CODE,
        ),
    ] {
        let mut action = action();
        action.operation_id = operation(label);
        action.topics = topics;
        let mut problems = Vec::new();
        crate::admin_action_validation::validate(
            &ScenarioAction::DeleteTopics(action),
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
fn transition_requires_matching_prior_topic_descriptions() {
    let mut scenario = scenario();
    scenario
        .steps
        .retain(|step| !matches!(&step.action, ScenarioAction::DescribeTopics(_)));
    let error = scenario
        .validate()
        .expect_err("plural deletion without prior descriptions");
    assert!(
        error
            .to_string()
            .contains("requires a matching prior topic description"),
        "{error}"
    );
}

fn action() -> DeleteTopicsAction {
    DeleteTopicsAction {
        client_id: client(),
        operation_id: operation("delete-topics"),
        topics: vec![
            expectation("topic-z", None),
            expectation("topic-missing", Some(UNKNOWN_TOPIC_OR_PARTITION_ERROR_CODE)),
            expectation("topic-a", None),
        ],
        timeout_ms: 1_000,
    }
}

fn command() -> DeleteTopicsCommand {
    DeleteTopicsCommand {
        client_id: client(),
        operation_id: operation("delete-topics"),
        topics: topic_names(),
        timeout_ms: 1_000,
    }
}

fn completion() -> AdminTopicsDeletion {
    AdminTopicsDeletion {
        operation_id: operation("delete-topics"),
        outcomes: vec![
            outcome("topic-z", None),
            outcome("topic-missing", Some(UNKNOWN_TOPIC_OR_PARTITION_ERROR_CODE)),
            outcome("topic-a", None),
        ],
    }
}

fn expectation(topic: &str, expected_error_code: Option<&str>) -> DeleteTopicExpectation {
    DeleteTopicExpectation {
        topic: topic.to_owned(),
        expected_error_code: expected_error_code.map(str::to_owned),
    }
}

fn outcome(topic: &str, error_code: Option<&str>) -> AdminTopicDeletionOutcome {
    AdminTopicDeletionOutcome {
        topic: topic.to_owned(),
        error_code: error_code.map(str::to_owned),
    }
}

fn topic_names() -> Vec<String> {
    vec![
        "topic-z".to_owned(),
        "topic-missing".to_owned(),
        "topic-a".to_owned(),
    ]
}

fn scenario() -> Scenario {
    toml::from_str(include_str!(
        "../../../scenarios/kafka/admin-delete-topics.toml"
    ))
    .unwrap_or_else(|error| panic!("parse plural topic deletion: {error}"))
}

fn round_trip<T>(value: &T)
where
    T: serde::Serialize + serde::de::DeserializeOwned + std::fmt::Debug + PartialEq,
{
    let encoded = serde_json::to_string(value)
        .unwrap_or_else(|error| panic!("encode plural topic deletion: {error}"));
    let decoded = serde_json::from_str::<T>(&encoded)
        .unwrap_or_else(|error| panic!("decode plural topic deletion: {error}"));
    assert_eq!(&decoded, value);
}

fn client() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client: {error}"))
}

fn operation(value: &str) -> OperationId {
    OperationId::new(value).unwrap_or_else(|error| panic!("operation: {error}"))
}
