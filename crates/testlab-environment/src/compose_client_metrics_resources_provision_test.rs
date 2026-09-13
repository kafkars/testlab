//! Tests for the compose client metrics resources provision contract.

use std::collections::BTreeSet;

use testlab_schema::Scenario;

#[test]
fn checked_in_scenario_yields_two_sorted_resource_names() {
    let scenario: Scenario = toml::from_str(include_str!(
        "../../../scenarios/kafka/admin-list-client-metrics-resources.toml"
    ))
    .unwrap_or_else(|error| panic!("parse client-metrics scenario: {error}"));
    assert_eq!(
        super::resources(&scenario),
        BTreeSet::from([
            "testlab-kafkars-client-metrics-alpha".to_owned(),
            "testlab-kafkars-client-metrics-zulu".to_owned(),
        ])
    );
}

#[test]
fn provision_command_uses_the_pinned_kafka_cli() {
    assert_eq!(
        super::command("broker-1", 19092, "metrics-a"),
        vec![
            "exec",
            "--no-TTY",
            "broker-1",
            "/opt/kafka/bin/kafka-client-metrics.sh",
            "--bootstrap-server",
            "localhost:19092",
            "--alter",
            "--name",
            "metrics-a",
            "--metrics",
            "org.apache.kafka.producer.",
            "--interval",
            "30000",
        ]
    );
}
