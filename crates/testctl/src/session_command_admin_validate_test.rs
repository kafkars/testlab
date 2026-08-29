//! Validate-only admin translation keeps verifier baselines off the wire and events distinct.

use testlab_schema::{
    AdapterCommand, AdapterEvent, AdminTopicCompletion, AdminTopicConfigCompletion,
    AlterTopicConfigAction, AlterTopicConfigCommand, ClientId, CreatePartitionsAction,
    CreatePartitionsCommand, CreateTopicAction, CreateTopicCommand, OperationId, ScenarioAction,
};

use crate::runner_protocol::{EventDisposition, ExpectedEvent};
use crate::session_command_admin::translate;

#[test]
fn topic_validation_translations_preserve_the_wire_flag() {
    let create_operation = operation("validate-create");
    let partition_operation = operation("validate-partitions");
    let cases = [
        (
            ScenarioAction::CreateTopic(CreateTopicAction {
                client_id: client(),
                operation_id: create_operation.clone(),
                topic: "orders".to_owned(),
                partitions: 1,
                replication_factor: 1,
                validate_only: true,
                expected_error_code: None,
                timeout_ms: 20_000,
            }),
            AdapterCommand::CreateTopic(CreateTopicCommand {
                client_id: client(),
                operation_id: create_operation.clone(),
                topic: "orders".to_owned(),
                partitions: 1,
                replication_factor: 1,
                validate_only: true,
                timeout_ms: 20_000,
            }),
            ExpectedEvent::TopicCreationValidated {
                operation_id: create_operation,
                topic: "orders".to_owned(),
            },
        ),
        (
            ScenarioAction::CreatePartitions(CreatePartitionsAction {
                client_id: client(),
                operation_id: partition_operation.clone(),
                topic: "orders".to_owned(),
                total_count: 3,
                validate_only: true,
                expected_current_count: Some(1),
                expected_error_code: None,
                timeout_ms: 20_000,
            }),
            AdapterCommand::CreatePartitions(CreatePartitionsCommand {
                client_id: client(),
                operation_id: partition_operation.clone(),
                topic: "orders".to_owned(),
                total_count: 3,
                validate_only: true,
                timeout_ms: 20_000,
            }),
            ExpectedEvent::TopicPartitionIncreaseValidated {
                operation_id: partition_operation,
                topic: "orders".to_owned(),
            },
        ),
    ];

    for (action, expected_command, expected_event) in cases {
        let Some((command, event)) = translate(&action) else {
            panic!("validate-only topic action must cross the adapter boundary");
        };
        assert_eq!(command, expected_command);
        assert!(same_expected_event(&event, &expected_event));
    }
}

#[test]
fn config_validation_keeps_the_expected_current_value_private() {
    let operation_id = operation("validate-config");
    let action = ScenarioAction::AlterTopicConfig(AlterTopicConfigAction {
        client_id: client(),
        operation_id: operation_id.clone(),
        topic: "orders".to_owned(),
        config_name: "cleanup.policy".to_owned(),
        value: "compact".to_owned(),
        validate_only: true,
        expected_current_value: Some("delete".to_owned()),
        timeout_ms: 20_000,
    });

    let Some((command, expected)) = translate(&action) else {
        panic!("validate-only config action must cross the adapter boundary");
    };
    assert_eq!(
        command,
        AdapterCommand::AlterTopicConfig(AlterTopicConfigCommand {
            client_id: client(),
            operation_id: operation_id.clone(),
            topic: "orders".to_owned(),
            config_name: "cleanup.policy".to_owned(),
            value: "compact".to_owned(),
            validate_only: true,
            timeout_ms: 20_000,
        })
    );
    assert!(matches!(
        expected,
        ExpectedEvent::TopicConfigAlterationValidated {
            operation_id: actual,
            topic,
            config_name,
        } if actual == operation_id && topic == "orders" && config_name == "cleanup.policy"
    ));
}

#[test]
fn validation_completion_is_not_interchangeable_with_mutation_completion() {
    let operation_id = operation("validate-create");
    let expected = ExpectedEvent::TopicCreationValidated {
        operation_id: operation_id.clone(),
        topic: "orders".to_owned(),
    };
    let validated = AdapterEvent::TopicCreationValidated(AdminTopicCompletion {
        operation_id: operation_id.clone(),
        topic: "orders".to_owned(),
    });
    let mutated = AdapterEvent::TopicCreated(AdminTopicCompletion {
        operation_id,
        topic: "orders".to_owned(),
    });

    assert_eq!(
        expected
            .classify(&validated)
            .unwrap_or_else(|error| panic!("validation classification: {error}")),
        EventDisposition::Complete
    );
    assert!(expected.classify(&mutated).is_err());

    let config_expected = ExpectedEvent::TopicConfigAlterationValidated {
        operation_id: operation("validate-config"),
        topic: "orders".to_owned(),
        config_name: "cleanup.policy".to_owned(),
    };
    assert!(
        config_expected
            .classify(&AdapterEvent::TopicConfigAltered(
                AdminTopicConfigCompletion {
                    operation_id: operation("validate-config"),
                    topic: "orders".to_owned(),
                    config_name: "cleanup.policy".to_owned(),
                }
            ))
            .is_err()
    );
}

fn same_expected_event(actual: &ExpectedEvent, expected: &ExpectedEvent) -> bool {
    matches!(
        (actual, expected),
        (
            ExpectedEvent::TopicCreationValidated {
                operation_id: actual_operation,
                topic: actual_topic,
            },
            ExpectedEvent::TopicCreationValidated {
                operation_id: expected_operation,
                topic: expected_topic,
            },
        ) | (
            ExpectedEvent::TopicPartitionIncreaseValidated {
                operation_id: actual_operation,
                topic: actual_topic,
            },
            ExpectedEvent::TopicPartitionIncreaseValidated {
                operation_id: expected_operation,
                topic: expected_topic,
            },
        ) if actual_operation == expected_operation && actual_topic == expected_topic
    )
}

fn client() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client ID: {error}"))
}

fn operation(value: &str) -> OperationId {
    OperationId::new(value).unwrap_or_else(|error| panic!("operation ID: {error}"))
}
