//! Plural topic-configuration schemas pin ordered expectation ownership.

use std::collections::{BTreeMap, BTreeSet};

use crate::{
    AdapterCommand, AdapterEvent, AdminTopicConfigDescriptionOutcome, AdminTopicConfigsDescription,
    ClientId, DescribeTopicConfigExpectation, DescribeTopicConfigsAction,
    DescribeTopicConfigsCommand, EVIDENCE_SCHEMA_VERSION, OperationId, PROTOCOL_VERSION,
    SCENARIO_SCHEMA_VERSION, Scenario, ScenarioAction, TopicConfigSelection,
};

#[test]
fn plural_topic_config_description_advances_all_versioned_boundaries() {
    assert_eq!(PROTOCOL_VERSION, 135);
    assert_eq!(SCENARIO_SCHEMA_VERSION, 139);
    assert_eq!(EVIDENCE_SCHEMA_VERSION, 125);
}

#[test]
fn action_command_and_completion_round_trip_in_caller_order() {
    round_trip(&ScenarioAction::DescribeTopicConfigs(action()));
    round_trip(&AdapterCommand::DescribeTopicConfigs(command()));
    round_trip(&AdapterEvent::TopicConfigsDescribed(completion()));
    let encoded = toml::to_string(&command())
        .unwrap_or_else(|error| panic!("encode plural topic configs: {error}"));
    assert!(!encoded.contains("expected_value"), "{encoded}");
}

#[test]
fn checked_in_plural_topic_config_scenario_is_valid() {
    scenario()
        .validate()
        .unwrap_or_else(|error| panic!("validate plural topic configs: {error}"));
}

#[test]
fn validation_rejects_singleton_duplicate_and_invalid_selection() {
    for (label, topics, expected) in [
        (
            "singleton",
            vec![expectation("topic-a", "cleanup.policy", "delete")],
            "between 2 and 32 entries",
        ),
        (
            "duplicate",
            vec![
                expectation("topic-a", "cleanup.policy", "delete"),
                expectation("topic-a", "retention.ms", "1000"),
            ],
            "unique valid names",
        ),
        (
            "key",
            vec![
                expectation("topic-a", "", "delete"),
                expectation("topic-b", "cleanup.policy", "delete"),
            ],
            "invalid config_name",
        ),
        (
            "value",
            vec![
                expectation("topic-a", "cleanup.policy", ""),
                expectation("topic-b", "cleanup.policy", "delete"),
            ],
            "configuration value must contain",
        ),
    ] {
        let mut action = action();
        action.operation_id = operation(label);
        action.topics = topics;
        let mut problems = Vec::new();
        crate::admin_config_action_validation::validate(
            &ScenarioAction::DescribeTopicConfigs(action),
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

fn action() -> DescribeTopicConfigsAction {
    DescribeTopicConfigsAction {
        client_id: client(),
        operation_id: operation("describe-topic-configs"),
        api: crate::TopicConfigApi::Topic,
        include_synonyms: true,
        include_documentation: true,
        topics: vec![
            expectation("topic-z", "cleanup.policy", "delete"),
            expectation("topic-a", "retention.ms", "604800000"),
        ],
        timeout_ms: 1_000,
    }
}

fn command() -> DescribeTopicConfigsCommand {
    DescribeTopicConfigsCommand {
        client_id: client(),
        operation_id: operation("describe-topic-configs"),
        api: crate::TopicConfigApi::Topic,
        include_synonyms: true,
        include_documentation: true,
        topics: vec![
            selection("topic-z", "cleanup.policy"),
            selection("topic-a", "retention.ms"),
        ],
        timeout_ms: 1_000,
    }
}

fn completion() -> AdminTopicConfigsDescription {
    AdminTopicConfigsDescription {
        operation_id: operation("describe-topic-configs"),
        outcomes: vec![
            outcome("topic-z", "cleanup.policy", "delete"),
            outcome("topic-a", "retention.ms", "604800000"),
        ],
    }
}

fn expectation(
    topic: &str,
    config_name: &str,
    expected_value: &str,
) -> DescribeTopicConfigExpectation {
    DescribeTopicConfigExpectation {
        topic: topic.to_owned(),
        config_name: config_name.to_owned(),
        expected_value: expected_value.to_owned(),
    }
}

fn selection(topic: &str, config_name: &str) -> TopicConfigSelection {
    TopicConfigSelection {
        topic: topic.to_owned(),
        config_name: config_name.to_owned(),
    }
}

fn outcome(topic: &str, config_name: &str, value: &str) -> AdminTopicConfigDescriptionOutcome {
    AdminTopicConfigDescriptionOutcome {
        topic: topic.to_owned(),
        config_name: config_name.to_owned(),
        value: Some(value.to_owned()),
        metadata: Some(crate::AdminConfigEntryMetadata {
            read_only: false,
            source: 5,
            sensitive: false,
            synonyms: vec![crate::AdminConfigSynonym {
                name: config_name.to_owned(),
                value: Some(value.to_owned()),
                source: 5,
            }],
            config_type: Some(7),
            documentation: Some("Topic configuration documentation".to_owned()),
        }),
        error_code: None,
    }
}

fn scenario() -> Scenario {
    toml::from_str(include_str!(
        "../../../scenarios/kafka/admin-describe-topic-configs.toml"
    ))
    .unwrap_or_else(|error| panic!("parse plural topic configs: {error}"))
}

fn round_trip<T>(value: &T)
where
    T: serde::Serialize + serde::de::DeserializeOwned + std::fmt::Debug + PartialEq,
{
    let encoded = serde_json::to_string(value)
        .unwrap_or_else(|error| panic!("encode plural topic configs: {error}"));
    let decoded = serde_json::from_str::<T>(&encoded)
        .unwrap_or_else(|error| panic!("decode plural topic configs: {error}"));
    assert_eq!(&decoded, value);
}

fn client() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client: {error}"))
}

fn operation(value: &str) -> OperationId {
    OperationId::new(value).unwrap_or_else(|error| panic!("operation: {error}"))
}
