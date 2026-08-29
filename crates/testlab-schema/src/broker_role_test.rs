//! Broker-role scenario tests pin target identity and initialization ordering.

use crate::Scenario;

#[test]
fn group_coordinator_requires_prior_group_initialization() {
    let error = validate_error(
        r#"
schema_version = 37
id = "fault.uninitialized-group"
title = "uninitialized group"
description = "group role target must already exist"
timeout_ms = 1000
requires = []
assertions = []

[[steps]]
id = "stop-role"
kind = "stop_broker_role"
timeout_ms = 500
[steps.target]
role = "group_coordinator"
group_id = "missing-group"

[[steps]]
id = "restore-role"
kind = "restore_broker_role"
timeout_ms = 500
[steps.target]
role = "group_coordinator"
group_id = "missing-group"
"#,
    );

    assert!(
        error
            .problems
            .iter()
            .any(|problem| problem.contains("was not initialized before its stop"))
    );
}

#[test]
fn partition_leader_requires_a_scenario_record_target() {
    let error = validate_error(
        r#"
schema_version = 37
id = "fault.missing-partition"
title = "missing partition"
description = "partition role target must belong to scenario intent"
timeout_ms = 1000
requires = []
assertions = []

[[steps]]
id = "stop-role"
kind = "stop_broker_role"
timeout_ms = 500
[steps.target]
role = "partition_leader"
topic = "records"
partition = 0

[[steps]]
id = "restore-role"
kind = "restore_broker_role"
timeout_ms = 500
[steps.target]
role = "partition_leader"
topic = "records"
partition = 0
"#,
    );

    assert!(
        error
            .problems
            .iter()
            .any(|problem| problem.contains("has no scenario record"))
    );
}

fn validate_error(source: &str) -> crate::ScenarioError {
    let scenario: Scenario =
        toml::from_str(source).unwrap_or_else(|error| panic!("parse role scenario: {error}"));
    match scenario.validate() {
        Ok(()) => panic!("invalid role target must fail"),
        Err(error) => error,
    }
}
