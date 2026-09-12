//! Plural topic-configuration protocol tests pin expectation-free caller order.

use testlab_schema::{
    AdapterCommand, AdapterEvent, AdminTopicConfigAlterationOutcome,
    AdminTopicConfigDescriptionOutcome, AdminTopicConfigsAlteration, AdminTopicConfigsDescription,
    AlterTopicConfigExpectation, AlterTopicConfigsAction, ClientId, DescribeTopicConfigExpectation,
    DescribeTopicConfigsAction, OperationId, ScenarioAction,
};

use crate::runner_protocol::{EventDisposition, ExpectedEvent};

#[test]
fn translation_preserves_order_without_expected_values() {
    let scenario_action = ScenarioAction::DescribeTopicConfigs(action());
    let Some((AdapterCommand::DescribeTopicConfigs(command), expected)) =
        crate::session_command_admin_config::translate(&scenario_action)
    else {
        panic!("plural topic-configuration translation");
    };
    assert_eq!(command.client_id, client());
    assert_eq!(command.operation_id, operation());
    assert!(command.include_synonyms);
    assert!(command.include_documentation);
    assert_eq!(
        command
            .topics
            .iter()
            .map(|selected| (selected.topic.as_str(), selected.config_name.as_str()))
            .collect::<Vec<_>>(),
        [("topic-z", "cleanup.policy"), ("topic-a", "retention.ms")]
    );
    let encoded = serde_json::to_string(&command)
        .unwrap_or_else(|error| panic!("encode plural topic configs: {error}"));
    assert!(!encoded.contains("expected_value"), "{encoded}");
    assert!(matches!(
        expected,
        ExpectedEvent::TopicConfigsDescribed { .. }
    ));
}

#[test]
fn completion_requires_every_selected_identity_in_order() {
    let expected = ExpectedEvent::TopicConfigsDescribed {
        operation_id: operation(),
        topics: vec![
            ("topic-z".to_owned(), "cleanup.policy".to_owned()),
            ("topic-a".to_owned(), "retention.ms".to_owned()),
        ],
    };
    let mut event = AdapterEvent::TopicConfigsDescribed(completion());
    assert_eq!(
        expected
            .classify(&event)
            .unwrap_or_else(|error| panic!("classify plural topic configs: {error}")),
        EventDisposition::Complete
    );
    let AdapterEvent::TopicConfigsDescribed(actual) = &mut event else {
        panic!("plural topic-configuration event");
    };
    actual.outcomes.swap(0, 1);
    let error = expected
        .classify(&event)
        .expect_err("reordered topic configurations must not complete");
    assert_eq!(error.harness_error().code, "event_identity_mismatch");
}

#[test]
fn mutation_translation_preserves_order_without_baseline_expectations() {
    let scenario_action = ScenarioAction::AlterTopicConfigs(mutation_action());
    let Some((AdapterCommand::AlterTopicConfigs(command), expected)) =
        crate::session_command_admin_config::translate(&scenario_action)
    else {
        panic!("plural topic-configuration mutation translation");
    };
    assert_eq!(command.operation_id, mutation_operation());
    assert_eq!(command.topics[0].topic, "topic-z");
    assert_eq!(command.topics[1].topic, "topic-a");
    let encoded = serde_json::to_string(&command)
        .unwrap_or_else(|error| panic!("encode plural configuration mutation: {error}"));
    assert!(!encoded.contains("baseline_operation_id"), "{encoded}");
    assert!(!encoded.contains("expected_previous_value"), "{encoded}");
    assert!(matches!(
        expected,
        ExpectedEvent::TopicConfigsAltered { .. }
    ));
}

#[test]
fn mutation_completion_requires_every_selected_identity_in_order() {
    let expected = ExpectedEvent::TopicConfigsAltered {
        operation_id: mutation_operation(),
        topics: vec![
            ("topic-z".to_owned(), "cleanup.policy".to_owned()),
            ("topic-a".to_owned(), "cleanup.policy".to_owned()),
        ],
    };
    let mut event = AdapterEvent::TopicConfigsAltered(mutation_completion());
    assert_eq!(
        expected
            .classify(&event)
            .unwrap_or_else(|error| panic!("classify plural configuration mutation: {error}")),
        EventDisposition::Complete
    );
    let AdapterEvent::TopicConfigsAltered(actual) = &mut event else {
        panic!("plural topic-configuration mutation event");
    };
    actual.outcomes.swap(0, 1);
    let error = expected
        .classify(&event)
        .expect_err("reordered mutation outcomes must not complete");
    assert_eq!(error.harness_error().code, "event_identity_mismatch");
}

#[test]
fn generic_resource_selection_crosses_both_wire_commands() {
    let mut description = action();
    description.api = testlab_schema::TopicConfigApi::Resource;
    let Some((AdapterCommand::DescribeTopicConfigs(description), _)) =
        crate::session_command_admin_config::translate(&ScenarioAction::DescribeTopicConfigs(
            description,
        ))
    else {
        panic!("generic description translation");
    };
    assert_eq!(description.api, testlab_schema::TopicConfigApi::Resource);

    let mut mutation = mutation_action();
    mutation.api = testlab_schema::TopicConfigMutationApi::Resource;
    let Some((AdapterCommand::AlterTopicConfigs(mutation), _)) =
        crate::session_command_admin_config::translate(&ScenarioAction::AlterTopicConfigs(
            mutation,
        ))
    else {
        panic!("generic mutation translation");
    };
    assert_eq!(
        mutation.api,
        testlab_schema::TopicConfigMutationApi::Resource
    );
}

#[test]
fn legacy_mutation_selectors_cross_the_wire_command() {
    for api in [
        testlab_schema::TopicConfigMutationApi::LegacyTopic,
        testlab_schema::TopicConfigMutationApi::LegacyResource,
    ] {
        let mut mutation = mutation_action();
        mutation.api = api;
        let Some((AdapterCommand::AlterTopicConfigs(command), _)) =
            crate::session_command_admin_config::translate(&ScenarioAction::AlterTopicConfigs(
                mutation,
            ))
        else {
            panic!("legacy mutation translation");
        };
        assert_eq!(command.api, api);
    }
}

#[test]
fn legacy_default_restore_keeps_expected_value_outside_the_wire_command() {
    let mut mutation = mutation_action();
    mutation.api = testlab_schema::TopicConfigMutationApi::LegacyTopic;
    for selected in &mut mutation.topics {
        selected.expected_previous_value = "compact".to_owned();
        selected.value = "delete".to_owned();
        selected.method = testlab_schema::TopicConfigMutationMethod::RestoreDefault;
    }
    let Some((AdapterCommand::AlterTopicConfigs(command), _)) =
        crate::session_command_admin_config::translate(&ScenarioAction::AlterTopicConfigs(
            mutation,
        ))
    else {
        panic!("legacy default restoration translation");
    };
    assert!(
        command
            .topics
            .iter()
            .all(|selected| selected.value.is_none())
    );
    let encoded = serde_json::to_string(&command)
        .unwrap_or_else(|error| panic!("encode legacy default restoration: {error}"));
    assert!(!encoded.contains("\"value\""), "{encoded}");
    assert!(!encoded.contains("delete"), "{encoded}");
}

fn action() -> DescribeTopicConfigsAction {
    DescribeTopicConfigsAction {
        client_id: client(),
        operation_id: operation(),
        api: testlab_schema::TopicConfigApi::Topic,
        include_synonyms: true,
        include_documentation: true,
        topics: vec![
            expectation("topic-z", "cleanup.policy", "delete"),
            expectation("topic-a", "retention.ms", "604800000"),
        ],
        timeout_ms: 1_000,
    }
}

fn completion() -> AdminTopicConfigsDescription {
    AdminTopicConfigsDescription {
        operation_id: operation(),
        outcomes: vec![
            outcome("topic-z", "cleanup.policy", "delete"),
            outcome("topic-a", "retention.ms", "604800000"),
        ],
    }
}

fn mutation_action() -> AlterTopicConfigsAction {
    AlterTopicConfigsAction {
        client_id: client(),
        operation_id: mutation_operation(),
        baseline_operation_id: operation(),
        api: testlab_schema::TopicConfigMutationApi::Topic,
        topics: vec![
            mutation("topic-z", "cleanup.policy"),
            mutation("topic-a", "cleanup.policy"),
        ],
        timeout_ms: 1_000,
    }
}

fn mutation_completion() -> AdminTopicConfigsAlteration {
    AdminTopicConfigsAlteration {
        operation_id: mutation_operation(),
        outcomes: vec![
            mutation_outcome("topic-z", "cleanup.policy"),
            mutation_outcome("topic-a", "cleanup.policy"),
        ],
    }
}

fn mutation(topic: &str, config_name: &str) -> AlterTopicConfigExpectation {
    AlterTopicConfigExpectation {
        topic: topic.to_owned(),
        config_name: config_name.to_owned(),
        expected_previous_value: "delete".to_owned(),
        value: "compact".to_owned(),
        method: testlab_schema::TopicConfigMutationMethod::Set,
        operation_value: None,
    }
}

fn mutation_outcome(topic: &str, config_name: &str) -> AdminTopicConfigAlterationOutcome {
    AdminTopicConfigAlterationOutcome {
        topic: topic.to_owned(),
        config_name: config_name.to_owned(),
        error_code: None,
    }
}

fn expectation(topic: &str, config_name: &str, value: &str) -> DescribeTopicConfigExpectation {
    DescribeTopicConfigExpectation {
        topic: topic.to_owned(),
        config_name: config_name.to_owned(),
        expected_value: value.to_owned(),
    }
}

fn outcome(topic: &str, config_name: &str, value: &str) -> AdminTopicConfigDescriptionOutcome {
    AdminTopicConfigDescriptionOutcome {
        topic: topic.to_owned(),
        config_name: config_name.to_owned(),
        value: Some(value.to_owned()),
        metadata: None,
        error_code: None,
    }
}

fn client() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client: {error}"))
}

fn operation() -> OperationId {
    OperationId::new("describe-topic-configs").unwrap_or_else(|error| panic!("operation: {error}"))
}

fn mutation_operation() -> OperationId {
    OperationId::new("alter-topic-configs").unwrap_or_else(|error| panic!("operation: {error}"))
}

#[path = "admin_topic_config_method_protocol_test.rs"]
mod method_test;
