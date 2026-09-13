//! Kafkars Admin catalog coverage stays visible across every supported pack.

use std::path::Path;

use crate::catalog::Repository;

#[test]
fn kafkars_pack_variants_retain_admin_discovery() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let repository = match Repository::open(&root) {
        Ok(repository) => repository,
        Err(error) => panic!("failed to open test repository: {error}"),
    };
    for path in [
        "packs/kafkars-pr.toml",
        "packs/kafkars-classic.toml",
        "packs/kafkars-release.toml",
        "packs/kafkars-share-release.toml",
        "packs/kafkars-three-broker.toml",
        "packs/kafkars-three-broker-share.toml",
        "packs/kafkars-three-broker-security.toml",
    ] {
        let (_, pack) = match repository.load_pack(Path::new(path)) {
            Ok(value) => value,
            Err(error) => panic!("load {path}: {error}"),
        };
        for scenario in [
            "admin-create-partitions.toml",
            "admin-create-topic-validate-only.toml",
            "admin-create-partitions-validate-only.toml",
            "admin-describe-topic.toml",
            "admin-describe-topics.toml",
            "admin-delete-topics.toml",
            "admin-topic-ids.toml",
            "admin-list-topics.toml",
            "admin-list-internal-topics.toml",
            "admin-list-offsets.toml",
            "admin-list-consumer-group-offsets.toml",
            "admin-create-topic-duplicate.toml",
            "admin-create-topics-batch-partial.toml",
            "admin-create-partitions-unknown-topic.toml",
            "admin-delete-topic-unknown-topic.toml",
            "admin-describe-topic-unknown-topic.toml",
            "admin-list-earliest-offset.toml",
            "admin-list-topics-multiple.toml",
            "admin-list-consumer-groups-multiple.toml",
            "admin-describe-classic-groups.toml",
            "admin-list-consumer-groups-offsets.toml",
            "admin-consumer-group-offsets-batch-lifecycle.toml",
            "admin-list-live-consumer-group-offset.toml",
            "admin-topic-lifecycle.toml",
            "admin-alter-consumer-group-offset-partition-isolation.toml",
            "admin-topic-config-lifecycle.toml",
            "admin-describe-topic-configs.toml",
            "admin-topic-config-validate-only.toml",
        ] {
            assert!(
                pack.scenarios
                    .iter()
                    .any(|candidate| candidate.ends_with(scenario)),
                "{path} omitted {scenario}"
            );
        }
    }
}

#[test]
fn transaction_pattern_filter_stays_on_kafka_4_3_packs() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let repository = Repository::open(&root)
        .unwrap_or_else(|error| panic!("failed to open test repository: {error}"));
    for path in [
        "packs/kafkars-pr.toml",
        "packs/kafkars-three-broker-share.toml",
    ] {
        let (_, pack) = repository
            .load_pack(Path::new(path))
            .unwrap_or_else(|error| panic!("load {path}: {error}"));
        assert!(
            pack.scenarios
                .iter()
                .any(|scenario| scenario.ends_with("admin-transaction-pattern-filter.toml")),
            "{path} omitted the Kafka 4.3 transaction-pattern filter"
        );
    }
    for path in [
        "packs/kafkars-classic.toml",
        "packs/kafkars-share-release.toml",
    ] {
        let (_, pack) = repository
            .load_pack(Path::new(path))
            .unwrap_or_else(|error| panic!("load {path}: {error}"));
        assert!(
            !pack
                .scenarios
                .iter()
                .any(|scenario| scenario.ends_with("admin-transaction-pattern-filter.toml"))
        );
    }
}
