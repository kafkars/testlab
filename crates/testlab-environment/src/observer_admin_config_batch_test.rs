//! Plural topic-configuration observation pins exact order and provisioning.

use std::collections::BTreeMap;

use testlab_schema::{
    AdapterCommand, ClientId, DescribeTopicConfigExpectation, DescribeTopicConfigsAction,
    DescribeTopicConfigsCommand, OperationId, Scenario, ScenarioAction, TopicConfigSelection,
};

use crate::observer_admin_target::{AdminTarget, ConfigBatchTarget, ConfigTarget};

#[test]
fn exact_action_and_expectation_free_command_map_to_ordered_targets() {
    let scenario_action = ScenarioAction::DescribeTopicConfigs(action());
    let command = AdapterCommand::DescribeTopicConfigs(command());
    let target = AdminTarget::from_exact(&scenario_action, &command)
        .unwrap_or_else(|error| panic!("map plural topic configurations: {error}"))
        .unwrap_or_else(|| panic!("plural topic-configuration target"));
    assert_eq!(target, expected_target());
    assert_eq!(target.observation_count(), 2);
}

#[test]
fn altered_wire_order_is_rejected_before_observation() {
    let scenario_action = ScenarioAction::DescribeTopicConfigs(action());
    let mut command = command();
    command.topics.swap(0, 1);
    let error = AdminTarget::from_exact(
        &scenario_action,
        &AdapterCommand::DescribeTopicConfigs(command),
    )
    .expect_err("mismatched plural topic-configuration order");
    assert!(error.to_string().contains("does not exactly match"));
}

#[test]
fn provisioning_creates_every_selected_topic_once() {
    let scenario: Scenario = toml::from_str(include_str!(
        "../../../scenarios/kafka/admin-describe-topic-configs.toml"
    ))
    .unwrap_or_else(|error| panic!("parse plural topic configurations: {error}"));
    assert_eq!(
        crate::compose_provision_targets::topics(&scenario),
        BTreeMap::from([
            (
                "testlab-kafkars-admin-describe-topic-configs-alpha".to_owned(),
                1,
            ),
            (
                "testlab-kafkars-admin-describe-topic-configs-zulu".to_owned(),
                1,
            ),
        ])
    );
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

fn command() -> DescribeTopicConfigsCommand {
    DescribeTopicConfigsCommand {
        client_id: client(),
        operation_id: operation(),
        topics: vec![
            selection("topic-z", "cleanup.policy"),
            selection("topic-a", "retention.ms"),
        ],
        timeout_ms: 1_000,
    }
}

fn expected_target() -> AdminTarget {
    AdminTarget::TopicConfigs(ConfigBatchTarget {
        operation_id: operation(),
        configs: vec![
            target("topic-z", "cleanup.policy", "delete"),
            target("topic-a", "retention.ms", "604800000"),
        ],
    })
}

fn expectation(topic: &str, config_name: &str, value: &str) -> DescribeTopicConfigExpectation {
    DescribeTopicConfigExpectation {
        topic: topic.to_owned(),
        config_name: config_name.to_owned(),
        expected_value: value.to_owned(),
    }
}

fn selection(topic: &str, config_name: &str) -> TopicConfigSelection {
    TopicConfigSelection {
        topic: topic.to_owned(),
        config_name: config_name.to_owned(),
    }
}

fn target(topic: &str, config_name: &str, value: &str) -> ConfigTarget {
    ConfigTarget {
        operation_id: operation(),
        topic: topic.to_owned(),
        config_name: config_name.to_owned(),
        expected_value: value.to_owned(),
        poll_expected: false,
    }
}

fn client() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client: {error}"))
}

fn operation() -> OperationId {
    OperationId::new("describe-topic-configs").unwrap_or_else(|error| panic!("operation: {error}"))
}
