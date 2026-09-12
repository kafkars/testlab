//! Plural topic-description schemas pin caller order and mixed resource outcomes.

use std::collections::{BTreeMap, BTreeSet};

use super::{
    AdapterCommand, AdapterEvent, AdminTopicDescriptionOutcome, AdminTopicDescriptionValue,
    AdminTopicPartitionDescriptionOutcome, AdminTopicsDescription, BrokerStateObservation,
    BrokerTopicIdentityState, ClientId, CreateTopicAction, DescribeTopicExpectation,
    DescribeTopicsAction, DescribeTopicsCommand, EVIDENCE_SCHEMA_VERSION, OperationId,
    PROTOCOL_VERSION, SCENARIO_SCHEMA_VERSION, Scenario, ScenarioAction, ScenarioStep, StepId,
    TopicSelection, UNKNOWN_TOPIC_OR_PARTITION_ERROR_CODE,
};

#[test]
fn plural_topic_description_advances_all_versioned_boundaries() {
    assert_eq!(PROTOCOL_VERSION, 112);
    assert_eq!(SCENARIO_SCHEMA_VERSION, 115);
    assert_eq!(EVIDENCE_SCHEMA_VERSION, 101);
}

#[test]
fn action_command_and_completion_round_trip_in_caller_order() {
    round_trip(&ScenarioAction::DescribeTopics(action()));
    round_trip(&AdapterCommand::DescribeTopics(command()));
    round_trip(&AdapterEvent::TopicsDescribed(completion()));
    round_trip(&BrokerStateObservation::TopicIdentity(
        BrokerTopicIdentityState {
            observation: 7,
            operation_id: operation("describe-topics-by-id"),
            topic: "topic-z".to_owned(),
            topic_id: [1; 16],
            partitions: vec![0, 1],
        },
    ));
    assert_eq!(command().topics, topic_names());
}

#[test]
fn checked_in_plural_topic_description_scenario_is_valid() {
    scenario()
        .validate()
        .unwrap_or_else(|error| panic!("validate plural topic description: {error}"));
}

#[test]
fn checked_in_topic_id_scenario_is_valid() {
    let scenario: Scenario = toml::from_str(include_str!(
        "../../../scenarios/kafka/admin-topic-ids.toml"
    ))
    .unwrap_or_else(|error| panic!("parse topic-ID scenario: {error}"));
    scenario
        .validate()
        .unwrap_or_else(|error| panic!("validate topic-ID scenario: {error}"));
}

#[test]
fn missing_topic_expectation_rejects_a_prior_successful_creation() {
    let mut scenario = scenario();
    scenario.steps.insert(
        2,
        ScenarioStep {
            id: StepId::new("create-missing-topic").unwrap_or_else(|error| panic!("step: {error}")),
            action: ScenarioAction::CreateTopic(CreateTopicAction {
                client_id: client(),
                operation_id: operation("create-missing-topic"),
                topic: "testlab-kafkars-admin-describe-topics-missing".to_owned(),
                partitions: 1,
                replication_factor: 1,
                validate_only: false,
                expected_error_code: None,
                timeout_ms: 1_000,
            }),
        },
    );
    let error = scenario
        .validate()
        .expect_err("created topic cannot satisfy a missing-topic expectation");
    assert!(
        error.to_string().contains("expects missing topic")
            && error.to_string().contains("prior action created it"),
        "{error}"
    );
}

#[test]
fn validation_rejects_nonplural_duplicate_and_ambiguous_outcomes() {
    for (label, topics, expected) in [
        (
            "singleton",
            vec![success("topic-a", vec![0])],
            "between 2 and 32 entries",
        ),
        (
            "duplicate",
            vec![success("topic-a", vec![0]), success("topic-a", vec![0])],
            "unique valid names",
        ),
        (
            "neither",
            vec![
                success("topic-a", vec![0]),
                expectation("topic-b", None, None),
            ],
            "exactly one public outcome",
        ),
        (
            "both",
            vec![
                success("topic-a", vec![0]),
                expectation(
                    "topic-b",
                    Some(vec![0]),
                    Some(UNKNOWN_TOPIC_OR_PARTITION_ERROR_CODE),
                ),
            ],
            "exactly one public outcome",
        ),
    ] {
        assert_invalid(label, topics, expected);
    }
}

#[test]
fn validation_rejects_invalid_partitions_and_error_codes() {
    assert_invalid(
        "partitions",
        vec![success("topic-a", vec![0]), success("topic-b", vec![1, 1])],
        "sorted unique nonnegative indices",
    );
    assert_invalid(
        "error",
        vec![
            success("topic-a", vec![0]),
            expectation("topic-b", None, Some("broker:broker_1")),
        ],
        UNKNOWN_TOPIC_OR_PARTITION_ERROR_CODE,
    );
}

#[test]
fn topic_id_selection_rejects_a_missing_lookup_name() {
    let mut action = action();
    action.selection = TopicSelection::TopicId;
    let mut problems = Vec::new();
    crate::admin_action_validation::validate(
        &ScenarioAction::DescribeTopics(action),
        &BTreeMap::from([(client(), false)]),
        &mut BTreeSet::new(),
        &mut problems,
    );
    assert!(
        problems
            .iter()
            .any(|problem| problem.contains("topic-ID lookup") && problem.contains("must exist")),
        "{problems:?}"
    );
}

fn assert_invalid(label: &str, topics: Vec<DescribeTopicExpectation>, expected: &str) {
    let mut action = action();
    action.operation_id = operation(label);
    action.topics = topics;
    let mut problems = Vec::new();
    crate::admin_action_validation::validate(
        &ScenarioAction::DescribeTopics(action),
        &BTreeMap::from([(client(), false)]),
        &mut BTreeSet::new(),
        &mut problems,
    );
    assert!(
        problems.iter().any(|problem| problem.contains(expected)),
        "{label}: {problems:?}"
    );
}

fn action() -> DescribeTopicsAction {
    DescribeTopicsAction {
        client_id: client(),
        operation_id: operation("describe-topics"),
        selection: TopicSelection::Name,
        topics: vec![
            success("topic-z", vec![0, 1]),
            failure("topic-missing"),
            success("topic-a", vec![0]),
        ],
        timeout_ms: 1_000,
    }
}

fn command() -> DescribeTopicsCommand {
    DescribeTopicsCommand {
        client_id: client(),
        operation_id: operation("describe-topics"),
        selection: TopicSelection::Name,
        topics: topic_names(),
        timeout_ms: 1_000,
    }
}

fn completion() -> AdminTopicsDescription {
    AdminTopicsDescription {
        operation_id: operation("describe-topics"),
        outcomes: vec![
            described("topic-z", vec![0, 1], 1),
            AdminTopicDescriptionOutcome {
                topic: "topic-missing".to_owned(),
                topic_id: None,
                description: None,
                error_code: Some(UNKNOWN_TOPIC_OR_PARTITION_ERROR_CODE.to_owned()),
            },
            described("topic-a", vec![0], 2),
        ],
    }
}

fn described(topic: &str, partitions: Vec<i32>, topic_id: u8) -> AdminTopicDescriptionOutcome {
    AdminTopicDescriptionOutcome {
        topic: topic.to_owned(),
        topic_id: None,
        description: Some(AdminTopicDescriptionValue {
            topic_id: Some([topic_id; 16]),
            internal: false,
            partitions: partitions
                .into_iter()
                .map(|partition| AdminTopicPartitionDescriptionOutcome {
                    partition,
                    error_code: None,
                })
                .collect(),
        }),
        error_code: None,
    }
}

fn success(topic: &str, partitions: Vec<i32>) -> DescribeTopicExpectation {
    expectation(topic, Some(partitions), None)
}

fn failure(topic: &str) -> DescribeTopicExpectation {
    expectation(topic, None, Some(UNKNOWN_TOPIC_OR_PARTITION_ERROR_CODE))
}

fn expectation(
    topic: &str,
    expected_partitions: Option<Vec<i32>>,
    expected_error_code: Option<&str>,
) -> DescribeTopicExpectation {
    DescribeTopicExpectation {
        topic: topic.to_owned(),
        expected_partitions,
        expected_error_code: expected_error_code.map(str::to_owned),
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
        "../../../scenarios/kafka/admin-describe-topics.toml"
    ))
    .unwrap_or_else(|error| panic!("parse plural topic description: {error}"))
}

fn round_trip<T>(value: &T)
where
    T: serde::Serialize + serde::de::DeserializeOwned + std::fmt::Debug + PartialEq,
{
    let encoded = serde_json::to_string(value)
        .unwrap_or_else(|error| panic!("encode plural topic description: {error}"));
    let decoded = serde_json::from_str::<T>(&encoded)
        .unwrap_or_else(|error| panic!("decode plural topic description: {error}"));
    assert_eq!(&decoded, value);
}

fn client() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client: {error}"))
}

fn operation(value: &str) -> OperationId {
    OperationId::new(value).unwrap_or_else(|error| panic!("operation: {error}"))
}
