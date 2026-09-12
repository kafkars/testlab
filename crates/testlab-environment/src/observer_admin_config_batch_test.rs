//! Plural topic-configuration observation pins exact order and provisioning.

use std::collections::BTreeMap;

use testlab_schema::{
    AdapterCommand, AlterTopicConfigExpectation, AlterTopicConfigsAction, AlterTopicConfigsCommand,
    ClientId, DescribeTopicConfigExpectation, DescribeTopicConfigsAction,
    DescribeTopicConfigsCommand, OperationId, Scenario, ScenarioAction, TopicConfigAlteration,
    TopicConfigSelection,
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

#[test]
fn plural_mutation_maps_replacements_to_ordered_polling_targets() {
    let scenario_action = ScenarioAction::AlterTopicConfigs(mutation_action());
    let command = AdapterCommand::AlterTopicConfigs(mutation_command());
    let target = AdminTarget::from_exact(&scenario_action, &command)
        .unwrap_or_else(|error| panic!("map plural configuration mutation: {error}"))
        .unwrap_or_else(|| panic!("plural configuration-mutation target"));
    assert_eq!(target, expected_mutation_target());
    assert_eq!(target.observation_count(), 2);
}

#[test]
fn mutation_scenario_provisions_every_selected_topic() {
    let scenario: Scenario = toml::from_str(include_str!(
        "../../../scenarios/kafka/admin-alter-topic-configs.toml"
    ))
    .unwrap_or_else(|error| panic!("parse plural configuration mutation: {error}"));
    assert_eq!(
        crate::compose_provision_targets::topics(&scenario),
        BTreeMap::from([
            (
                "testlab-kafkars-admin-alter-topic-configs-alpha".to_owned(),
                1,
            ),
            (
                "testlab-kafkars-admin-alter-topic-configs-zulu".to_owned(),
                1,
            ),
        ])
    );
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

fn command() -> DescribeTopicConfigsCommand {
    DescribeTopicConfigsCommand {
        client_id: client(),
        operation_id: operation(),
        api: testlab_schema::TopicConfigApi::Topic,
        include_synonyms: true,
        include_documentation: true,
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

fn mutation_action() -> AlterTopicConfigsAction {
    AlterTopicConfigsAction {
        client_id: client(),
        operation_id: mutation_operation(),
        baseline_operation_id: operation(),
        api: testlab_schema::TopicConfigMutationApi::Topic,
        topics: vec![
            mutation_expectation("topic-z", "cleanup.policy"),
            mutation_expectation("topic-a", "cleanup.policy"),
        ],
        timeout_ms: 1_000,
    }
}

fn mutation_command() -> AlterTopicConfigsCommand {
    AlterTopicConfigsCommand {
        client_id: client(),
        operation_id: mutation_operation(),
        api: testlab_schema::TopicConfigMutationApi::Topic,
        topics: vec![
            mutation("topic-z", "cleanup.policy"),
            mutation("topic-a", "cleanup.policy"),
        ],
        timeout_ms: 1_000,
    }
}

fn expected_mutation_target() -> AdminTarget {
    AdminTarget::TopicConfigs(ConfigBatchTarget {
        operation_id: mutation_operation(),
        configs: vec![
            mutation_target("topic-z", "cleanup.policy"),
            mutation_target("topic-a", "cleanup.policy"),
        ],
    })
}

fn mutation_expectation(topic: &str, config_name: &str) -> AlterTopicConfigExpectation {
    AlterTopicConfigExpectation {
        topic: topic.to_owned(),
        config_name: config_name.to_owned(),
        expected_previous_value: "delete".to_owned(),
        value: "compact".to_owned(),
        method: testlab_schema::TopicConfigMutationMethod::Set,
        operation_value: None,
    }
}

fn mutation(topic: &str, config_name: &str) -> TopicConfigAlteration {
    TopicConfigAlteration {
        topic: topic.to_owned(),
        config_name: config_name.to_owned(),
        method: testlab_schema::TopicConfigMutationMethod::Set,
        value: Some("compact".to_owned()),
    }
}

fn mutation_target(topic: &str, config_name: &str) -> ConfigTarget {
    ConfigTarget {
        operation_id: mutation_operation(),
        topic: topic.to_owned(),
        config_name: config_name.to_owned(),
        expected_value: "compact".to_owned(),
        poll_expected: true,
    }
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

fn mutation_operation() -> OperationId {
    OperationId::new("alter-topic-configs").unwrap_or_else(|error| panic!("operation: {error}"))
}
