//! Plural topic-configuration protocol tests pin expectation-free caller order.

use testlab_schema::{
    AdapterCommand, AdapterEvent, AdminTopicConfigDescriptionOutcome, AdminTopicConfigsDescription,
    ClientId, DescribeTopicConfigExpectation, DescribeTopicConfigsAction, OperationId,
    ScenarioAction,
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

fn action() -> DescribeTopicConfigsAction {
    DescribeTopicConfigsAction {
        client_id: client(),
        operation_id: operation(),
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
        error_code: None,
    }
}

fn client() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client: {error}"))
}

fn operation() -> OperationId {
    OperationId::new("describe-topic-configs").unwrap_or_else(|error| panic!("operation: {error}"))
}
