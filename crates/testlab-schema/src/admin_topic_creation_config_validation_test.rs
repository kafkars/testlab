//! Tests for the admin topic creation config validation contract.

use crate::{ClientId, CreateTopicAction, OperationId, TopicCreationConfig};

use super::validate;

#[test]
fn creation_configs_require_unique_bounded_names_and_values() {
    let mut action = configured_action();
    action.configs.push(TopicCreationConfig {
        name: "cleanup.policy".to_owned(),
        value: String::new(),
    });
    let mut problems = Vec::new();

    validate(&action, &mut problems);

    assert!(
        problems
            .iter()
            .any(|problem| problem.contains("value must contain 1 to 32768 bytes"))
    );
    assert!(
        problems
            .iter()
            .any(|problem| problem.contains("config names must be unique"))
    );
}

fn configured_action() -> CreateTopicAction {
    CreateTopicAction {
        client_id: ClientId::new("client-1").unwrap_or_else(|error| panic!("client ID: {error}")),
        operation_id: OperationId::new("configured-create")
            .unwrap_or_else(|error| panic!("operation ID: {error}")),
        topic: "orders".to_owned(),
        partitions: 1,
        replication_factor: 1,
        replica_assignments: None,
        configs: vec![TopicCreationConfig {
            name: "cleanup.policy".to_owned(),
            value: "compact".to_owned(),
        }],
        validate_only: false,
        expected_error_code: None,
        timeout_ms: 1_000,
    }
}
