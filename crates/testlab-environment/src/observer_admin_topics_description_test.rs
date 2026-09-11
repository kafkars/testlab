//! Plural topic-description observation pins exact caller order and provisioning intent.

use std::collections::BTreeMap;

use testlab_schema::{
    AdapterCommand, ClientId, DescribeTopicExpectation, DescribeTopicsAction,
    DescribeTopicsCommand, OperationId, Scenario, ScenarioAction,
    UNKNOWN_TOPIC_OR_PARTITION_ERROR_CODE,
};

use crate::observer_admin_target::{AdminTarget, ListTarget};

#[test]
fn exact_action_and_expectation_free_command_map_to_caller_ordered_topics() {
    let action = ScenarioAction::DescribeTopics(action());
    let command = AdapterCommand::DescribeTopics(command());
    let target = AdminTarget::from_exact(&action, &command)
        .unwrap_or_else(|error| panic!("map plural topic descriptions: {error}"))
        .unwrap_or_else(|| panic!("plural topic-description target"));
    assert_eq!(target, expected_target());
    assert_eq!(target.observation_count(), 3);
}

#[test]
fn altered_wire_order_is_rejected_before_observation() {
    let action = ScenarioAction::DescribeTopics(action());
    let mut command = command();
    command.topics.swap(0, 1);
    let error = AdminTarget::from_exact(&action, &AdapterCommand::DescribeTopics(command))
        .expect_err("mismatched plural topic-description order");
    assert!(error.to_string().contains("does not exactly match"));
}

#[test]
fn provisioning_creates_only_topics_expected_to_succeed() {
    let scenario: Scenario = toml::from_str(include_str!(
        "../../../scenarios/kafka/admin-describe-topics.toml"
    ))
    .unwrap_or_else(|error| panic!("parse plural topic-description scenario: {error}"));
    assert_eq!(
        crate::compose_provision_targets::topics(&scenario),
        BTreeMap::from([
            ("testlab-kafkars-admin-describe-topics-alpha".to_owned(), 1),
            ("testlab-kafkars-admin-describe-topics-zulu".to_owned(), 2),
        ])
    );
}

fn action() -> DescribeTopicsAction {
    DescribeTopicsAction {
        client_id: client(),
        operation_id: operation(),
        topics: vec![
            expectation("topic-z", Some(vec![0, 1]), None),
            expectation(
                "topic-missing",
                None,
                Some(UNKNOWN_TOPIC_OR_PARTITION_ERROR_CODE),
            ),
            expectation("topic-a", Some(vec![0]), None),
        ],
        timeout_ms: 1_000,
    }
}

fn command() -> DescribeTopicsCommand {
    DescribeTopicsCommand {
        client_id: client(),
        operation_id: operation(),
        topics: topic_names(),
        timeout_ms: 1_000,
    }
}

fn expected_target() -> AdminTarget {
    AdminTarget::Topics(ListTarget {
        operation_id: operation(),
        names: topic_names(),
    })
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

fn client() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client: {error}"))
}

fn operation() -> OperationId {
    OperationId::new("describe-topics").unwrap_or_else(|error| panic!("operation: {error}"))
}
