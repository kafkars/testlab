//! Plural configuration-mutation schemas keep baselines explicit and off the wire.

use std::collections::{BTreeMap, BTreeSet};

use crate::{
    AdapterCommand, AdapterEvent, AdminTopicConfigAlterationOutcome, AdminTopicConfigsAlteration,
    AlterTopicConfigExpectation, AlterTopicConfigsAction, AlterTopicConfigsCommand, ClientId,
    EVIDENCE_SCHEMA_VERSION, OperationId, PROTOCOL_VERSION, SCENARIO_SCHEMA_VERSION, Scenario,
    ScenarioAction, TopicConfigAlteration,
};

#[test]
fn plural_topic_config_mutation_advances_all_versioned_boundaries() {
    assert_eq!(PROTOCOL_VERSION, 147);
    assert_eq!(SCENARIO_SCHEMA_VERSION, 151);
    assert_eq!(EVIDENCE_SCHEMA_VERSION, 137);
}

#[test]
fn action_command_and_completion_round_trip_without_baseline_leakage() {
    round_trip(&ScenarioAction::AlterTopicConfigs(action()));
    round_trip(&AdapterCommand::AlterTopicConfigs(command()));
    round_trip(&AdapterEvent::TopicConfigsAltered(completion()));

    let encoded = serde_json::to_string(&command())
        .unwrap_or_else(|error| panic!("encode plural configuration mutation: {error}"));
    assert!(!encoded.contains("baseline_operation_id"), "{encoded}");
    assert!(!encoded.contains("expected_previous_value"), "{encoded}");
    assert!(!encoded.contains("\"api\""), "{encoded}");
    assert_eq!(command().topics[0].topic, "topic-z");
    assert_eq!(completion().outcomes[1].topic, "topic-a");
}

#[test]
fn legacy_selectors_round_trip_and_map_to_matching_description_surfaces() {
    for (api, encoded_name, description_api) in [
        (
            crate::TopicConfigMutationApi::LegacyTopic,
            "legacy_topic",
            crate::TopicConfigApi::Topic,
        ),
        (
            crate::TopicConfigMutationApi::LegacyResource,
            "legacy_resource",
            crate::TopicConfigApi::Resource,
        ),
    ] {
        let mut command = command();
        command.api = api;
        round_trip(&command);
        let encoded = serde_json::to_string(&command)
            .unwrap_or_else(|error| panic!("encode legacy selector: {error}"));
        assert!(encoded.contains(encoded_name), "{encoded}");
        assert_eq!(api.description_api(), description_api);
    }
}

#[test]
fn checked_in_plural_topic_config_mutation_is_valid() {
    scenario()
        .validate()
        .unwrap_or_else(|error| panic!("validate plural configuration mutation: {error}"));
}

#[test]
fn validation_rejects_singleton_duplicate_self_baseline_and_empty_values() {
    let clients = BTreeMap::from([(client(), false)]);
    let cases = [
        (
            "singleton",
            vec![expectation(
                "topic-z",
                "cleanup.policy",
                "delete",
                "compact",
            )],
            false,
            "between 2 and 32 entries",
        ),
        (
            "duplicate",
            vec![
                expectation("topic-z", "cleanup.policy", "delete", "compact"),
                expectation("topic-z", "retention.ms", "1000", "2000"),
            ],
            false,
            "unique valid names",
        ),
        (
            "empty",
            vec![
                expectation("topic-z", "cleanup.policy", "", "compact"),
                expectation("topic-a", "cleanup.policy", "delete", ""),
            ],
            false,
            "configuration value must contain",
        ),
        ("self-baseline", expectations(), true, "cannot use itself"),
    ];
    for (label, topics, self_baseline, expected) in cases {
        let mut action = action();
        action.operation_id = operation(label);
        action.topics = topics;
        if self_baseline {
            action.baseline_operation_id = action.operation_id.clone();
        }
        let mut problems = Vec::new();
        crate::admin_config_action_validation::validate(
            &ScenarioAction::AlterTopicConfigs(action),
            &clients,
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
fn transition_requires_the_named_exact_distinct_unmodified_baseline() {
    let mut valid = scenario();
    let mut problems = Vec::new();
    crate::admin_config_transition_validation::validate(&valid, &mut problems);
    assert!(problems.is_empty(), "{problems:?}");

    let ScenarioAction::AlterTopicConfigs(alter) = &mut valid.steps[3].action else {
        panic!("alter action kind");
    };
    alter.baseline_operation_id = operation("missing-baseline");
    problems.clear();
    crate::admin_config_transition_validation::validate(&valid, &mut problems);
    assert_problem(&problems, "requires prior plural");

    let mut mismatched = scenario();
    let ScenarioAction::AlterTopicConfigs(alter) = &mut mismatched.steps[3].action else {
        panic!("alter action kind");
    };
    alter.topics[0].expected_previous_value = "compact".to_owned();
    problems.clear();
    crate::admin_config_transition_validation::validate(&mismatched, &mut problems);
    assert_problem(&problems, "does not exactly match");
    assert_problem(&problems, "requires a different prior value");

    let mut mismatched_api = scenario();
    let ScenarioAction::AlterTopicConfigs(alter) = &mut mismatched_api.steps[3].action else {
        panic!("alter action kind");
    };
    alter.api = crate::TopicConfigMutationApi::Resource;
    problems.clear();
    crate::admin_config_transition_validation::validate(&mismatched_api, &mut problems);
    assert_problem(&problems, "does not exactly match");
}

fn action() -> AlterTopicConfigsAction {
    AlterTopicConfigsAction {
        client_id: client(),
        operation_id: operation("alter-topic-configs"),
        baseline_operation_id: operation("describe-topic-configs-before"),
        api: crate::TopicConfigMutationApi::Topic,
        topics: expectations(),
        timeout_ms: 1_000,
    }
}

fn command() -> AlterTopicConfigsCommand {
    AlterTopicConfigsCommand {
        client_id: client(),
        operation_id: operation("alter-topic-configs"),
        api: crate::TopicConfigMutationApi::Topic,
        topics: vec![
            alteration("topic-z", "cleanup.policy", "compact"),
            alteration("topic-a", "cleanup.policy", "compact"),
        ],
        timeout_ms: 1_000,
    }
}

fn completion() -> AdminTopicConfigsAlteration {
    AdminTopicConfigsAlteration {
        operation_id: operation("alter-topic-configs"),
        outcomes: vec![
            outcome("topic-z", "cleanup.policy"),
            outcome("topic-a", "cleanup.policy"),
        ],
    }
}

fn expectations() -> Vec<AlterTopicConfigExpectation> {
    vec![
        expectation("topic-z", "cleanup.policy", "delete", "compact"),
        expectation("topic-a", "cleanup.policy", "delete", "compact"),
    ]
}

fn expectation(
    topic: &str,
    config_name: &str,
    expected_previous_value: &str,
    value: &str,
) -> AlterTopicConfigExpectation {
    AlterTopicConfigExpectation {
        topic: topic.to_owned(),
        config_name: config_name.to_owned(),
        expected_previous_value: expected_previous_value.to_owned(),
        value: value.to_owned(),
        method: crate::TopicConfigMutationMethod::Set,
        operation_value: None,
    }
}

fn alteration(topic: &str, config_name: &str, value: &str) -> TopicConfigAlteration {
    TopicConfigAlteration {
        topic: topic.to_owned(),
        config_name: config_name.to_owned(),
        method: crate::TopicConfigMutationMethod::Set,
        value: Some(value.to_owned()),
    }
}

fn outcome(topic: &str, config_name: &str) -> AdminTopicConfigAlterationOutcome {
    AdminTopicConfigAlterationOutcome {
        topic: topic.to_owned(),
        config_name: config_name.to_owned(),
        error_code: None,
    }
}

fn scenario() -> Scenario {
    toml::from_str(include_str!(
        "../../../scenarios/kafka/admin-alter-topic-configs.toml"
    ))
    .unwrap_or_else(|error| panic!("parse plural configuration mutation: {error}"))
}

fn round_trip<T>(value: &T)
where
    T: serde::Serialize + serde::de::DeserializeOwned + std::fmt::Debug + PartialEq,
{
    let encoded = serde_json::to_string(value)
        .unwrap_or_else(|error| panic!("encode plural configuration mutation: {error}"));
    let decoded = serde_json::from_str::<T>(&encoded)
        .unwrap_or_else(|error| panic!("decode plural configuration mutation: {error}"));
    assert_eq!(&decoded, value);
}

fn assert_problem(problems: &[String], expected: &str) {
    assert!(
        problems.iter().any(|problem| problem.contains(expected)),
        "missing {expected:?} in {problems:?}"
    );
}

fn client() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client: {error}"))
}

fn operation(value: &str) -> OperationId {
    OperationId::new(value).unwrap_or_else(|error| panic!("operation: {error}"))
}

#[path = "admin_config_mutation_method_test.rs"]
mod mutation_method_test;
