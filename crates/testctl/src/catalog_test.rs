//! Catalog tests exercise the checked-in manifest graph as one unit.

use std::path::Path;

use crate::catalog::Repository;

#[test]
fn checked_in_catalog_is_complete() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let repository = match Repository::open(&root) {
        Ok(repository) => repository,
        Err(error) => panic!("failed to open test repository: {error}"),
    };
    let summary = match repository.validate_all() {
        Ok(summary) => summary,
        Err(error) => panic!("catalog validation failed: {error}"),
    };
    assert_eq!(summary.scenarios, 135);
    assert_eq!(summary.packs, 26);
    assert_eq!(summary.subjects, 2);
    assert_eq!(summary.environments, 20);
    assert_eq!(summary.qualifications, 3);
    assert_eq!(summary.contracts, 113);
}

#[test]
fn release_cells_use_their_topology_pack() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let repository = match Repository::open(&root) {
        Ok(repository) => repository,
        Err(error) => panic!("failed to open test repository: {error}"),
    };
    let (_, qualification) =
        match repository.load_qualification(Path::new("qualifications/kafkars-release.toml")) {
            Ok(value) => value,
            Err(error) => panic!("load release qualification: {error}"),
        };
    for cell in qualification.cells {
        let legacy = cell.environment.contains("apache-kafka/3.");
        let three_plaintext = cell.environment.ends_with("three-plaintext.toml");
        let three_security = cell.environment.contains("/three-") && !three_plaintext;
        let kafka_4_0 = cell.environment.contains("apache-kafka/4.0.");
        let expected = if legacy {
            "packs/kafkars-classic.toml"
        } else if kafka_4_0 {
            "packs/kafkars-release.toml"
        } else if three_plaintext {
            "packs/kafkars-three-broker-share.toml"
        } else if three_security {
            "packs/kafkars-three-broker-security.toml"
        } else {
            "packs/kafkars-share-release.toml"
        };
        assert_eq!(cell.pack, expected, "unexpected pack for {}", cell.id);
    }
    let (_, pack) = match repository.load_pack(Path::new("packs/kafkars-classic.toml")) {
        Ok(value) => value,
        Err(error) => panic!("load classic pack: {error}"),
    };
    assert!(
        !pack
            .scenarios
            .iter()
            .any(|scenario| scenario.contains("consumer-protocol"))
    );
    assert!(
        pack.scenarios
            .iter()
            .any(|scenario| scenario.ends_with("producer-broker-restart.toml"))
    );
    assert!(
        pack.scenarios
            .iter()
            .any(|scenario| scenario.ends_with("classic-group-broker-restart.toml"))
    );
    assert!(
        pack.scenarios
            .iter()
            .any(|scenario| scenario.ends_with("classic-group-record-fidelity.toml"))
    );
    assert!(!pack.scenarios.iter().any(|scenario| {
        scenario.ends_with("consumer-protocol-group-record-fidelity.toml")
            || scenario.ends_with("share-group-record-fidelity.toml")
    }));
    let (_, pack) = match repository.load_pack(Path::new("packs/kafkars-three-broker-share.toml")) {
        Ok(value) => value,
        Err(error) => panic!("load three-broker pack: {error}"),
    };
    assert!(
        pack.scenarios
            .iter()
            .any(|scenario| scenario.ends_with("producer-rolling-restart.toml"))
    );
    assert!(
        !pack
            .scenarios
            .iter()
            .any(|scenario| scenario.ends_with("producer-broker-restart.toml"))
    );
    assert!(
        pack.scenarios
            .iter()
            .any(|scenario| scenario.ends_with("share-group-leader-recovery.toml"))
    );
}

#[test]
fn pull_request_pack_excludes_release_disruptions() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let repository = match Repository::open(&root) {
        Ok(repository) => repository,
        Err(error) => panic!("failed to open test repository: {error}"),
    };
    let (_, pack) = match repository.load_pack(Path::new("packs/kafkars-pr.toml")) {
        Ok(value) => value,
        Err(error) => panic!("load pull-request pack: {error}"),
    };

    assert_eq!(pack.scenarios.len(), 90);
    assert!(
        !pack
            .scenarios
            .iter()
            .any(|scenario| scenario.contains("restart") || scenario.contains("fencing"))
    );
    assert!(
        pack.scenarios
            .iter()
            .any(|scenario| scenario.ends_with("share-group-membership-ownership.toml"))
    );
    assert_eq!(
        pack.scenarios
            .iter()
            .filter(|scenario| scenario.contains("/concurrent-"))
            .count(),
        3
    );
    for scenario in [
        "producer-null-empty-batch.toml",
        "producer-sequential-ordering.toml",
        "producer-repeated-readiness-flush.toml",
        "client-shutdown-isolation.toml",
        "assigned-consumer-null-empty.toml",
        "assigned-consumer-header-fidelity.toml",
        "assigned-consumer-sequential-cursor.toml",
        "assigned-consumer-replacement.toml",
        "assigned-consumer-beginning-reset.toml",
        "classic-group-record-fidelity.toml",
        "consumer-protocol-group-record-fidelity.toml",
        "share-group-record-fidelity.toml",
        "transaction-multi-record-commit.toml",
        "transaction-multi-record-abort.toml",
        "transaction-successive-boundaries.toml",
    ] {
        assert!(
            pack.scenarios
                .iter()
                .any(|candidate| candidate.ends_with(scenario)),
            "pull-request pack omitted {scenario}"
        );
    }
}

#[test]
fn kafkars_pack_variants_retain_supported_assigned_consumer_cursors() {
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
        "packs/kafkars-assigned-consumer-cursors.toml",
    ] {
        let (_, pack) = match repository.load_pack(Path::new(path)) {
            Ok(value) => value,
            Err(error) => panic!("load {path}: {error}"),
        };
        for scenario in [
            "assigned-consumer-sequential-cursor.toml",
            "assigned-consumer-replacement.toml",
            "assigned-consumer-beginning-reset.toml",
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
fn kafkars_pack_variants_retain_transaction_sets() {
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
        "packs/kafkars-transactions.toml",
    ] {
        let (_, pack) = match repository.load_pack(Path::new(path)) {
            Ok(value) => value,
            Err(error) => panic!("load {path}: {error}"),
        };
        for scenario in [
            "transaction-multi-record-commit.toml",
            "transaction-multi-record-abort.toml",
            "transaction-successive-boundaries.toml",
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
            "admin-list-topics.toml",
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
