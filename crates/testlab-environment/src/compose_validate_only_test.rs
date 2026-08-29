//! Validate-only provisioning tests pin the independently observable pre-state.

use std::collections::BTreeMap;

use testlab_schema::Scenario;

use crate::compose_provision_targets::topics;

#[test]
fn validate_only_admin_actions_provision_pre_state_without_the_created_topic() {
    let scenario: Scenario = toml::from_str(
        r#"
schema_version = 37
id = "kafka.admin-validate-only-provisioning"
title = "validate-only provisioning"
description = "validate-only provisioning fixture"
timeout_ms = 1000
requires = ["admin", "lifecycle"]
assertions = []

[[steps]]
id = "create"
kind = "create_topic"
client_id = "client-1"
operation_id = "validate-create"
topic = "new-orders"
partitions = 3
replication_factor = 1
validate_only = true
timeout_ms = 500

[[steps]]
id = "partitions"
kind = "create_partitions"
client_id = "client-1"
operation_id = "validate-partitions"
topic = "existing-orders"
total_count = 4
validate_only = true
expected_current_count = 2
timeout_ms = 500

[[steps]]
id = "config"
kind = "alter_topic_config"
client_id = "client-1"
operation_id = "validate-config"
topic = "configured-orders"
config_name = "cleanup.policy"
value = "compact"
validate_only = true
expected_current_value = "delete"
timeout_ms = 500
"#,
    )
    .unwrap_or_else(|error| panic!("parse validate-only scenario: {error}"));

    assert_eq!(
        topics(&scenario),
        BTreeMap::from([
            ("configured-orders".to_owned(), 1),
            ("existing-orders".to_owned(), 2),
        ])
    );
}
