//! Topic-configuration harness tests pin wire intent and completion identity.

use testlab_schema::{
    AdapterCommand, AdapterEvent, AdminTopicConfigCompletion, AdminTopicConfigDescription,
    AlterTopicConfigAction, AlterTopicConfigCommand, ClientId, DescribeTopicConfigAction,
    DescribeTopicConfigCommand, OperationId, ScenarioAction,
};

use crate::runner_protocol::{EventDisposition, ExpectedEvent};
use crate::session_command_admin::translate;

#[test]
fn describe_translation_omits_expected_value() {
    let client_id = client();
    let operation_id = operation("config-describe");
    let action = ScenarioAction::DescribeTopicConfig(DescribeTopicConfigAction {
        client_id: client_id.clone(),
        operation_id: operation_id.clone(),
        topic: "orders".to_owned(),
        config_name: "cleanup.policy".to_owned(),
        expected_value: "delete".to_owned(),
        timeout_ms: 20_000,
    });

    let Some((command, _)) = translate(&action) else {
        panic!("topic-configuration description must cross the adapter boundary");
    };
    assert_eq!(
        command,
        AdapterCommand::DescribeTopicConfig(DescribeTopicConfigCommand {
            client_id,
            operation_id,
            topic: "orders".to_owned(),
            config_name: "cleanup.policy".to_owned(),
            timeout_ms: 20_000,
        })
    );
    let encoded = toml::to_string(&command)
        .unwrap_or_else(|error| panic!("serialize describe command: {error}"));
    assert!(!encoded.contains("expected_value"), "{encoded}");
}

#[test]
fn alter_translation_preserves_replacement_value() {
    let client_id = client();
    let operation_id = operation("config-alter");
    let action = ScenarioAction::AlterTopicConfig(AlterTopicConfigAction {
        client_id: client_id.clone(),
        operation_id: operation_id.clone(),
        topic: "orders".to_owned(),
        config_name: "cleanup.policy".to_owned(),
        value: "compact".to_owned(),
        validate_only: false,
        expected_current_value: None,
        timeout_ms: 20_000,
    });

    let Some((command, _)) = translate(&action) else {
        panic!("topic-configuration alteration must cross the adapter boundary");
    };
    assert_eq!(
        command,
        AdapterCommand::AlterTopicConfig(AlterTopicConfigCommand {
            client_id,
            operation_id,
            topic: "orders".to_owned(),
            config_name: "cleanup.policy".to_owned(),
            value: "compact".to_owned(),
            validate_only: false,
            timeout_ms: 20_000,
        })
    );
}

#[test]
fn config_completions_match_every_stable_identity() {
    let cases = [
        (
            ExpectedEvent::TopicConfigDescribed {
                operation_id: operation("config-describe"),
                topic: "orders".to_owned(),
                config_name: "cleanup.policy".to_owned(),
            },
            AdapterEvent::TopicConfigDescribed(AdminTopicConfigDescription {
                operation_id: operation("config-describe"),
                topic: "orders".to_owned(),
                config_name: "cleanup.policy".to_owned(),
                value: Some("delete".to_owned()),
            }),
        ),
        (
            ExpectedEvent::TopicConfigAltered {
                operation_id: operation("config-alter"),
                topic: "orders".to_owned(),
                config_name: "cleanup.policy".to_owned(),
            },
            AdapterEvent::TopicConfigAltered(AdminTopicConfigCompletion {
                operation_id: operation("config-alter"),
                topic: "orders".to_owned(),
                config_name: "cleanup.policy".to_owned(),
            }),
        ),
    ];

    for (expected, event) in cases {
        assert_eq!(
            expected
                .classify(&event)
                .unwrap_or_else(|error| panic!("config completion classification: {error}")),
            EventDisposition::Complete
        );
    }
}

#[test]
fn config_completion_identity_mismatch_is_a_protocol_failure() {
    let expected = ExpectedEvent::TopicConfigDescribed {
        operation_id: operation("config-describe"),
        topic: "orders".to_owned(),
        config_name: "cleanup.policy".to_owned(),
    };
    let mismatches = [
        AdminTopicConfigDescription {
            operation_id: operation("other-operation"),
            topic: "orders".to_owned(),
            config_name: "cleanup.policy".to_owned(),
            value: Some("delete".to_owned()),
        },
        AdminTopicConfigDescription {
            operation_id: operation("config-describe"),
            topic: "payments".to_owned(),
            config_name: "cleanup.policy".to_owned(),
            value: Some("delete".to_owned()),
        },
        AdminTopicConfigDescription {
            operation_id: operation("config-describe"),
            topic: "orders".to_owned(),
            config_name: "retention.ms".to_owned(),
            value: Some("delete".to_owned()),
        },
    ];

    for mismatch in mismatches {
        let Err(failure) = expected.classify(&AdapterEvent::TopicConfigDescribed(mismatch)) else {
            panic!("mismatched config completion must fail");
        };
        assert_eq!(failure.harness_error().code, "event_identity_mismatch");
    }
}

fn client() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client ID: {error}"))
}

fn operation(value: &str) -> OperationId {
    OperationId::new(value).unwrap_or_else(|error| panic!("operation ID: {error}"))
}
