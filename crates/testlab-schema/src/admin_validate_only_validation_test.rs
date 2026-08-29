//! Validate-only schema tests pin wire ownership and nonmutating transition semantics.

use std::collections::{BTreeMap, BTreeSet};

use super::{
    AdapterCommand, AdapterEvent, AdminTopicCompletion, AdminTopicConfigCompletion,
    AlterTopicConfigAction, AlterTopicConfigCommand, Capability, ClientId, CreatePartitionsAction,
    CreatePartitionsCommand, CreateTopicAction, CreateTopicCommand, DescribeTopicConfigAction,
    OperationId, Scenario, ScenarioAction, ScenarioId, ScenarioStep, StepId,
    TOPIC_ALREADY_EXISTS_ERROR_CODE,
};

#[test]
fn validation_flags_cross_the_wire_but_current_state_expectations_do_not() {
    let partition_action =
        ScenarioAction::CreatePartitions(partitions("partitions", 3, true, Some(1)));
    let partition_command = AdapterCommand::CreatePartitions(CreatePartitionsCommand {
        client_id: client(),
        operation_id: operation("partitions"),
        topic: "orders".to_owned(),
        total_count: 3,
        validate_only: true,
        timeout_ms: 1_000,
    });
    let config_action = ScenarioAction::AlterTopicConfig(config("config", true, Some("delete")));
    let config_command = AdapterCommand::AlterTopicConfig(AlterTopicConfigCommand {
        client_id: client(),
        operation_id: operation("config"),
        topic: "orders".to_owned(),
        config_name: "cleanup.policy".to_owned(),
        value: "compact".to_owned(),
        validate_only: true,
        timeout_ms: 1_000,
    });

    let partition_action = encode(&partition_action);
    let partition_command = encode(&partition_command);
    let config_action = encode(&config_action);
    let config_command = encode(&config_command);
    assert!(partition_action.contains("expected_current_count = 1"));
    assert!(partition_command.contains("validate_only = true"));
    assert!(!partition_command.contains("expected_current_count"));
    assert!(config_action.contains("expected_current_value = \"delete\""));
    assert!(config_command.contains("validate_only = true"));
    assert!(!config_command.contains("expected_current_value"));
}

#[test]
fn validate_only_commands_and_events_have_distinct_protocol_shapes() {
    let create = AdapterCommand::CreateTopic(CreateTopicCommand {
        client_id: client(),
        operation_id: operation("create"),
        topic: "orders".to_owned(),
        partitions: 1,
        replication_factor: 1,
        validate_only: true,
        timeout_ms: 1_000,
    });
    let topic = AdminTopicCompletion {
        operation_id: operation("create"),
        topic: "orders".to_owned(),
    };
    let config = AdminTopicConfigCompletion {
        operation_id: operation("config"),
        topic: "orders".to_owned(),
        config_name: "cleanup.policy".to_owned(),
    };

    assert!(encode(&create).contains("validate_only = true"));
    assert!(
        encode(&AdapterEvent::TopicCreationValidated(topic.clone()))
            .contains("kind = \"topic_creation_validated\"")
    );
    assert!(
        encode(&AdapterEvent::TopicPartitionIncreaseValidated(topic))
            .contains("kind = \"topic_partition_increase_validated\"")
    );
    assert!(
        encode(&AdapterEvent::TopicConfigAlterationValidated(config))
            .contains("kind = \"topic_config_alteration_validated\"")
    );
}

#[test]
fn action_validation_requires_coherent_validate_only_expectations() {
    let clients = BTreeMap::from([(client(), false)]);
    let mut problems = Vec::new();
    let mut operations = BTreeSet::new();
    let mut create = create_topic("create", true);
    create.expected_error_code = Some(TOPIC_ALREADY_EXISTS_ERROR_CODE.to_owned());
    crate::admin_topic_action_validation::validate(
        &ScenarioAction::CreateTopic(create),
        &clients,
        &mut operations,
        &mut problems,
    );
    let mut partitions = partitions("partitions", 3, true, Some(3));
    partitions.expected_error_code = Some(crate::UNKNOWN_TOPIC_OR_PARTITION_ERROR_CODE.to_owned());
    crate::admin_topic_action_validation::validate(
        &ScenarioAction::CreatePartitions(partitions),
        &clients,
        &mut operations,
        &mut problems,
    );
    crate::admin_config_action_validation::validate(
        &ScenarioAction::AlterTopicConfig(config("config", false, Some("compact"))),
        &clients,
        &mut operations,
        &mut problems,
    );

    for expected in [
        "validate_only cannot declare expected_error_code",
        "expected_current_count must be positive and less than total_count",
        "expected_current_value exactly when validate_only is true",
        "expected_current_value must differ",
    ] {
        assert!(
            problems.iter().any(|problem| problem.contains(expected)),
            "missing {expected:?} in {problems:?}"
        );
    }
}

#[test]
fn partition_validation_requires_exact_prior_count_and_does_not_mutate() {
    let valid = scenario(vec![
        ScenarioAction::CreateTopic(create_topic("create", false)),
        ScenarioAction::CreatePartitions(partitions("validate-three", 3, true, Some(1))),
        ScenarioAction::CreatePartitions(partitions("validate-four", 4, true, Some(1))),
    ]);
    let mut problems = Vec::new();
    crate::admin_transition_validation::validate(&valid, &mut problems);
    assert!(problems.is_empty(), "{problems:?}");

    let missing = scenario(vec![
        ScenarioAction::CreateTopic(create_topic("validate-create", true)),
        ScenarioAction::CreatePartitions(partitions("validate-partitions", 3, true, Some(1))),
    ]);
    crate::admin_transition_validation::validate(&missing, &mut problems);
    assert!(
        problems
            .iter()
            .any(|problem| problem.contains("prior exact partition count"))
    );
}

#[test]
fn config_validation_requires_exact_prior_value_and_does_not_mutate() {
    let valid = scenario(vec![
        ScenarioAction::DescribeTopicConfig(description("delete")),
        ScenarioAction::AlterTopicConfig(config("validate-config", true, Some("delete"))),
        ScenarioAction::AlterTopicConfig(config("alter-config", false, None)),
    ]);
    let mut problems = Vec::new();
    crate::admin_config_transition_validation::validate(&valid, &mut problems);
    assert!(problems.is_empty(), "{problems:?}");

    let mismatched = scenario(vec![
        ScenarioAction::DescribeTopicConfig(description("delete")),
        ScenarioAction::AlterTopicConfig(config("validate-mismatch", true, Some("compact,delete"))),
    ]);
    crate::admin_config_transition_validation::validate(&mismatched, &mut problems);
    assert!(
        problems
            .iter()
            .any(|problem| problem.contains("prior exact topic-configuration"))
    );
}

fn create_topic(operation_id: &str, validate_only: bool) -> CreateTopicAction {
    CreateTopicAction {
        client_id: client(),
        operation_id: operation(operation_id),
        topic: "orders".to_owned(),
        partitions: 1,
        replication_factor: 1,
        validate_only,
        expected_error_code: None,
        timeout_ms: 1_000,
    }
}

fn partitions(
    operation_id: &str,
    total_count: i32,
    validate_only: bool,
    expected_current_count: Option<i32>,
) -> CreatePartitionsAction {
    CreatePartitionsAction {
        client_id: client(),
        operation_id: operation(operation_id),
        topic: "orders".to_owned(),
        total_count,
        validate_only,
        expected_current_count,
        expected_error_code: None,
        timeout_ms: 1_000,
    }
}

fn config(
    operation_id: &str,
    validate_only: bool,
    expected_current_value: Option<&str>,
) -> AlterTopicConfigAction {
    AlterTopicConfigAction {
        client_id: client(),
        operation_id: operation(operation_id),
        topic: "orders".to_owned(),
        config_name: "cleanup.policy".to_owned(),
        value: "compact".to_owned(),
        validate_only,
        expected_current_value: expected_current_value.map(str::to_owned),
        timeout_ms: 1_000,
    }
}

fn description(expected_value: &str) -> DescribeTopicConfigAction {
    DescribeTopicConfigAction {
        client_id: client(),
        operation_id: operation("describe-config"),
        topic: "orders".to_owned(),
        config_name: "cleanup.policy".to_owned(),
        expected_value: expected_value.to_owned(),
        timeout_ms: 1_000,
    }
}

fn scenario(actions: Vec<ScenarioAction>) -> Scenario {
    Scenario {
        schema_version: crate::SCENARIO_SCHEMA_VERSION,
        id: ScenarioId::new("validate-only-test")
            .unwrap_or_else(|error| panic!("scenario ID: {error}")),
        title: "validate only test".to_owned(),
        description: "validate only test".to_owned(),
        timeout_ms: 5_000,
        requires: BTreeSet::from([Capability::Admin]),
        steps: actions
            .into_iter()
            .enumerate()
            .map(|(index, action)| ScenarioStep {
                id: StepId::new(format!("step-{index}"))
                    .unwrap_or_else(|error| panic!("step ID: {error}")),
                action,
            })
            .collect(),
        assertions: Vec::new(),
    }
}

fn encode<T: serde::Serialize>(value: &T) -> String {
    toml::to_string(value).unwrap_or_else(|error| panic!("serialize value: {error}"))
}

fn client() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client ID: {error}"))
}

fn operation(value: &str) -> OperationId {
    OperationId::new(value).unwrap_or_else(|error| panic!("operation ID: {error}"))
}
