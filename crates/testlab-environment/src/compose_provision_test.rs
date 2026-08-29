//! Provisioning tests pin topology-sized topic replication evidence.

use std::collections::{BTreeMap, BTreeSet};

use testlab_schema::Scenario;

use crate::compose_provision::operation_args;
use crate::compose_provision_targets::{SeedTarget, seed_targets, share_groups, topics};

#[test]
fn operation_records_cluster_replication_factor() {
    let topics = BTreeMap::from([("orders".to_owned(), 3)]);
    let args = operation_args(
        "127.0.0.1:19091,127.0.0.1:19092",
        &topics,
        &BTreeSet::new(),
        &BTreeSet::new(),
        2,
    );
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
fn share_groups_are_preconfigured_for_deterministic_earliest_start() {
    let scenario: Scenario = toml::from_str(
        r#"
schema_version = 37
id = "share.provisioning"
title = "share provisioning"
description = "share group provisioning fixture"
timeout_ms = 1000
requires = ["share_consumer", "lifecycle"]
assertions = []

[[steps]]
id = "first"
kind = "create_share_consumer"
client_id = "client-1"
consumer_id = "share-1"
group_id = "orders-share"
topic = "orders"
membership_timeout_ms = 1000
close_timeout_ms = 1000

[[steps]]
id = "second"
kind = "create_share_consumer"
client_id = "client-2"
consumer_id = "share-2"
group_id = "orders-share"
topic = "orders"
membership_timeout_ms = 1000
close_timeout_ms = 1000
"#,
    )
    .unwrap_or_else(|error| panic!("parse scenario: {error}"));
    let groups = share_groups(&scenario);
    assert_eq!(groups, BTreeSet::from(["orders-share".to_owned()]));
    assert_eq!(
        operation_args(
            "localhost:9092",
            &BTreeMap::new(),
            &groups,
            &BTreeSet::new(),
            1,
        ),
        [
            "--bootstrap-server",
            "localhost:9092",
            "--readiness-topic",
            "testlab-environment-readiness",
            "--require-full-isr",
            "--share-group",
            "orders-share",
            "--share-auto-offset-reset",
            "earliest",
        ]
    );
}

#[test]
fn delete_records_targets_are_seeded_and_recorded_in_provisioning_evidence() {
    let scenario: Scenario = toml::from_str(include_str!(
        "../../../scenarios/kafka/admin-delete-records.toml"
    ))
    .unwrap_or_else(|error| panic!("parse delete records scenario: {error}"));
    let seeds = seed_targets(&scenario);
    assert_eq!(
        seeds,
        BTreeSet::from([SeedTarget {
            topic: "testlab-kafkars-admin-delete-records".to_owned(),
            partition: 0,
            record_count: 3,
        }])
    );
    assert_eq!(
        operation_args(
            "localhost:9092",
            &topics(&scenario),
            &BTreeSet::new(),
            &seeds,
            1,
        )
        .into_iter()
        .rev()
        .take(6)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect::<Vec<_>>(),
        [
            "--seed-topic",
            "testlab-kafkars-admin-delete-records",
            "--seed-partition",
            "0",
            "--seed-record-count",
            "3",
        ]
    );
}

#[test]
fn batch_records_contribute_every_topic_partition() {
    let scenario: Scenario = toml::from_str(
        r#"
schema_version = 37
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
fn expected_admin_failures_provision_only_the_known_list_offsets_topic() {
    for manifest in [
        include_str!("../../../scenarios/kafka/admin-create-partitions-unknown-topic.toml"),
        include_str!("../../../scenarios/kafka/admin-delete-topic-unknown-topic.toml"),
        include_str!("../../../scenarios/kafka/admin-describe-topic-unknown-topic.toml"),
    ] {
        let scenario: Scenario =
            toml::from_str(manifest).unwrap_or_else(|error| panic!("parse scenario: {error}"));
        assert!(topics(&scenario).is_empty());
    }
    let scenario: Scenario = toml::from_str(include_str!(
        "../../../scenarios/kafka/admin-list-offsets-invalid-partition.toml"
    ))
    .unwrap_or_else(|error| panic!("parse list offsets scenario: {error}"));
    assert_eq!(
        topics(&scenario),
        BTreeMap::from([(
            "testlab-kafkars-admin-offsets-invalid-partition".to_owned(),
            1,
        )])
    );
}

#[test]
fn every_batch_created_topic_is_excluded_from_harness_provisioning() {
    let scenario: Scenario = toml::from_str(include_str!(
        "../../../scenarios/kafka/admin-create-topics-batch-partial.toml"
    ))
    .unwrap_or_else(|error| panic!("parse batch creation scenario: {error}"));

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
        (
            "../../scenarios/kafka/admin-list-consumer-group-offsets.toml",
            BTreeMap::from([("testlab-kafkars-admin-group-offsets".to_owned(), 1)]),
        ),
        (
            "../../scenarios/kafka/admin-topic-config-lifecycle.toml",
            BTreeMap::from([("testlab-kafkars-admin-topic-config".to_owned(), 1)]),
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

#[test]
fn describe_then_delete_without_records_is_preprovisioned() {
    let scenario: Scenario = toml::from_str(
        r#"
schema_version = 37
id = "kafka.admin-describe-delete"
title = "describe and delete"
description = "provisioning fixture"
timeout_ms = 1000
requires = ["admin", "lifecycle"]
assertions = []

[[steps]]
id = "client"
kind = "create_client"
client_id = "client-1"

[[steps]]
id = "describe"
kind = "describe_topic"
client_id = "client-1"
operation_id = "describe-topic"
topic = "orders"
expected_partitions = [0, 1, 2]
timeout_ms = 500

[[steps]]
id = "delete"
kind = "delete_topic"
client_id = "client-1"
operation_id = "delete-topic"
topic = "orders"
timeout_ms = 500
"#,
    )
    .unwrap_or_else(|error| panic!("parse scenario: {error}"));

    assert_eq!(
        topics(&scenario),
        BTreeMap::from([("orders".to_owned(), 3)])
    );
}
