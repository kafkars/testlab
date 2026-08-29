//! Topic-configuration observer tests preserve exact independent values.

use testlab_schema::{
    AdapterCommand, AlterTopicConfigAction, BrokerStateObservation, ClientId,
    DescribeTopicConfigAction, OperationId, ScenarioAction,
};

use crate::observer_admin_config::normalize_fixture;
use crate::observer_admin_target::ConfigTarget;

fn target() -> ConfigTarget {
    ConfigTarget {
        operation_id: OperationId::new("config-op")
            .unwrap_or_else(|error| panic!("operation ID: {error}")),
        topic: "orders".to_owned(),
        config_name: "cleanup.policy".to_owned(),
        expected_value: "compact".to_owned(),
        poll_expected: true,
    }
}

#[test]
fn exact_non_sensitive_value_is_retained() {
    let observed = normalize_fixture(
        7,
        &target(),
        "orders",
        vec![("cleanup.policy", Some("compact"), false)],
    )
    .unwrap_or_else(|error| panic!("valid observation: {error}"));
    let BrokerStateObservation::TopicConfig(value) = observed else {
        panic!("wrong observation kind");
    };
    assert_eq!(value.observation, 7);
    assert_eq!(value.value, "compact");
}

#[test]
fn mismatched_resource_is_rejected() {
    let Err(error) = normalize_fixture(
        7,
        &target(),
        "payments",
        vec![("cleanup.policy", Some("compact"), false)],
    ) else {
        panic!("mismatched topic must fail");
    };
    assert!(error.to_string().contains("unexpected topic resource"));
}

#[test]
fn unavailable_sensitive_value_is_not_manufactured() {
    let Err(error) =
        normalize_fixture(7, &target(), "orders", vec![("cleanup.policy", None, true)])
    else {
        panic!("sensitive value must invalidate observation");
    };
    assert!(
        error
            .to_string()
            .contains("marked the selected configuration sensitive")
    );
}

#[test]
fn config_targets_require_exact_wire_identity_and_poll_only_after_mutation() {
    let describe = ScenarioAction::DescribeTopicConfig(DescribeTopicConfigAction {
        client_id: client(),
        operation_id: operation(),
        topic: "orders".to_owned(),
        config_name: "cleanup.policy".to_owned(),
        expected_value: "delete".to_owned(),
        timeout_ms: 1_000,
    });
    let (mut command, describe_target) =
        crate::observer_admin_config_target::match_action(&describe)
            .unwrap_or_else(|error| panic!("valid config target: {error}"))
            .unwrap_or_else(|| panic!("config target"));
    let crate::observer_admin_target::AdminTarget::TopicConfig(describe_target) = describe_target
    else {
        panic!("config target kind");
    };
    assert!(!describe_target.poll_expected);
    assert!(
        crate::observer_admin_target::AdminTarget::from_exact(&describe, &command)
            .unwrap_or_else(|error| panic!("exact target: {error}"))
            .is_some()
    );
    let AdapterCommand::DescribeTopicConfig(payload) = &mut command else {
        panic!("config command kind");
    };
    payload.config_name = "retention.ms".to_owned();
    assert!(crate::observer_admin_target::AdminTarget::from_exact(&describe, &command).is_err());

    let alter = ScenarioAction::AlterTopicConfig(AlterTopicConfigAction {
        client_id: client(),
        operation_id: operation(),
        topic: "orders".to_owned(),
        config_name: "cleanup.policy".to_owned(),
        value: "compact".to_owned(),
        validate_only: false,
        expected_current_value: None,
        timeout_ms: 1_000,
    });
    let (_, alter_target) = crate::observer_admin_config_target::match_action(&alter)
        .unwrap_or_else(|error| panic!("valid config target: {error}"))
        .unwrap_or_else(|| panic!("config target"));
    let crate::observer_admin_target::AdminTarget::TopicConfig(alter_target) = alter_target else {
        panic!("config target kind");
    };
    assert!(alter_target.poll_expected);
}

fn client() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client ID: {error}"))
}

fn operation() -> OperationId {
    OperationId::new("config-op").unwrap_or_else(|error| panic!("operation ID: {error}"))
}
