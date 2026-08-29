//! Validate-only observer tests pin exact commands and unchanged broker state.

use testlab_schema::{
    AdapterCommand, AlterTopicConfigAction, ClientId, CreatePartitionsAction, CreateTopicAction,
    OperationId, ScenarioAction,
};

use crate::observer_admin_target::AdminTarget;

#[test]
fn topic_creation_targets_absence_without_polling() {
    let action = ScenarioAction::CreateTopic(CreateTopicAction {
        client_id: client(),
        operation_id: operation("validate-create"),
        topic: "new-orders".to_owned(),
        partitions: 3,
        replication_factor: 1,
        validate_only: true,
        expected_error_code: None,
        timeout_ms: 500,
    });
    let (mut command, AdminTarget::Topic(target)) = topic_match(&action) else {
        panic!("validate-only create topic target kind");
    };
    assert!(!target.expected_exists);
    assert_eq!(target.expected_partitions, Some(Vec::new()));
    assert!(!target.poll_expected);

    let AdapterCommand::CreateTopic(payload) = &mut command else {
        panic!("validate-only create topic command kind");
    };
    payload.validate_only = false;
    assert!(AdminTarget::from_exact(&action, &command).is_err());
}

#[test]
fn partition_increase_targets_current_topology_without_polling() {
    let action = ScenarioAction::CreatePartitions(CreatePartitionsAction {
        client_id: client(),
        operation_id: operation("validate-partitions"),
        topic: "orders".to_owned(),
        total_count: 4,
        validate_only: true,
        expected_current_count: Some(2),
        expected_error_code: None,
        timeout_ms: 500,
    });
    let (mut command, AdminTarget::Topic(target)) = topic_match(&action) else {
        panic!("validate-only partitions target kind");
    };
    assert!(target.expected_exists);
    assert_eq!(target.expected_partitions, Some(vec![0, 1]));
    assert!(!target.poll_expected);

    let AdapterCommand::CreatePartitions(payload) = &mut command else {
        panic!("validate-only partitions command kind");
    };
    payload.validate_only = false;
    assert!(AdminTarget::from_exact(&action, &command).is_err());
}

#[test]
fn configuration_alteration_targets_current_value_without_polling() {
    let action = ScenarioAction::AlterTopicConfig(AlterTopicConfigAction {
        client_id: client(),
        operation_id: operation("validate-config"),
        topic: "orders".to_owned(),
        config_name: "cleanup.policy".to_owned(),
        value: "compact".to_owned(),
        validate_only: true,
        expected_current_value: Some("delete".to_owned()),
        timeout_ms: 500,
    });
    let (mut command, AdminTarget::TopicConfig(target)) = config_match(&action) else {
        panic!("validate-only config target kind");
    };
    assert_eq!(target.expected_value, "delete");
    assert!(!target.poll_expected);

    let AdapterCommand::AlterTopicConfig(payload) = &mut command else {
        panic!("validate-only config command kind");
    };
    payload.validate_only = false;
    assert!(AdminTarget::from_exact(&action, &command).is_err());
}

fn topic_match(action: &ScenarioAction) -> (AdapterCommand, AdminTarget) {
    crate::observer_admin_topic_target::match_action(action)
        .unwrap_or_else(|error| panic!("valid topic target: {error}"))
        .unwrap_or_else(|| panic!("topic target"))
}

fn config_match(action: &ScenarioAction) -> (AdapterCommand, AdminTarget) {
    crate::observer_admin_config_target::match_action(action)
        .unwrap_or_else(|error| panic!("valid config target: {error}"))
        .unwrap_or_else(|| panic!("config target"))
}

fn client() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client ID: {error}"))
}

fn operation(value: &str) -> OperationId {
    OperationId::new(value).unwrap_or_else(|error| panic!("operation ID: {error}"))
}
