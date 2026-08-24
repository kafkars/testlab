//! Provisioning tests pin topology-sized topic replication evidence.

use std::collections::BTreeMap;

use testlab_schema::Scenario;

use crate::compose_provision::{operation_args, topics};

#[test]
fn operation_records_cluster_replication_factor() {
    let topics = BTreeMap::from([("orders".to_owned(), 3)]);

    let args = operation_args("127.0.0.1:19091,127.0.0.1:19092", &topics, 2);

    assert_eq!(
        args,
        [
            "--bootstrap-server",
            "127.0.0.1:19091,127.0.0.1:19092",
            "--readiness-topic",
            "testlab-environment-readiness",
            "--require-full-isr",
            "--topic",
            "orders",
            "--partitions",
            "3",
            "--replication-factor",
            "2",
        ]
    );
}

#[test]
fn batch_records_contribute_every_topic_partition() {
    let scenario: Scenario = toml::from_str(
        r#"
schema_version = 12
id = "producer.batch-topics"
title = "batch topics"
description = "batch provisioning fixture"
timeout_ms = 1000
requires = ["producer", "producer_batch", "lifecycle"]
assertions = []

[[steps]]
id = "batch"
kind = "send_batch"
producer_id = "producer-1"
operations = [
  { operation_id = "op-1", record = { topic = "orders", partition = 0, sequence = 1 } },
  { operation_id = "op-2", record = { topic = "orders", partition = 2, sequence = 2 } },
  { operation_id = "op-3", record = { topic = "audit", partition = 1, sequence = 3 } },
]
"#,
    )
    .unwrap_or_else(|error| panic!("parse scenario: {error}"));

    assert_eq!(
        topics(&scenario),
        BTreeMap::from([("audit".to_owned(), 2), ("orders".to_owned(), 3)])
    );
}

#[test]
fn admin_created_and_expanded_topics_are_not_preprovisioned() {
    let scenario: Scenario = toml::from_str(include_str!(
        "../../../scenarios/kafka/admin-create-partitions.toml"
    ))
    .unwrap_or_else(|error| panic!("parse scenario: {error}"));

    assert!(topics(&scenario).is_empty());
}

#[test]
fn read_only_admin_topics_are_preprovisioned_from_their_markers() {
    for (path, expected) in [
        (
            "../../scenarios/kafka/admin-describe-topic.toml",
            BTreeMap::from([("testlab-kafkars-admin-described".to_owned(), 3)]),
        ),
        (
            "../../scenarios/kafka/admin-list-topics.toml",
            BTreeMap::from([("testlab-kafkars-admin-listed".to_owned(), 1)]),
        ),
        (
            "../../scenarios/kafka/admin-list-offsets.toml",
            BTreeMap::from([("testlab-kafkars-admin-offsets".to_owned(), 1)]),
        ),
    ] {
        let manifest =
            std::fs::read_to_string(std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(path))
                .unwrap_or_else(|error| panic!("read {path}: {error}"));
        let scenario: Scenario =
            toml::from_str(&manifest).unwrap_or_else(|error| panic!("parse {path}: {error}"));

        assert_eq!(topics(&scenario), expected, "unexpected topics for {path}");
    }
}

#[test]
fn fenced_transaction_record_contributes_its_topic() {
    let scenario: Scenario = toml::from_str(include_str!(
        "../../../scenarios/kafka/transaction-fencing.toml"
    ))
    .unwrap_or_else(|error| panic!("parse fencing scenario: {error}"));

    assert_eq!(
        topics(&scenario),
        BTreeMap::from([("testlab-kafkars-transaction-fencing".to_owned(), 1)])
    );
}
