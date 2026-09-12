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
    assert_eq!(summary.scenarios, 186);
    assert_eq!(summary.packs, 28);
    assert_eq!(summary.subjects, 2);
    assert_eq!(summary.environments, 23);
    assert_eq!(summary.qualifications, 3);
    assert_eq!(summary.contracts, 170);
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
        let expected = if cell.environment.ends_with("protocol-adversary.toml") {
            "packs/kafkars-protocol-adversary.toml"
        } else if cell.environment.ends_with("single-plaintext-network.toml") {
            "packs/kafkars-network-faults.toml"
        } else if cell.environment.ends_with("single-sasl-plain-policy.toml") {
            "packs/kafkars-broker-policy.toml"
        } else if cell.pack == "packs/kafkars-broker-role-failover.toml" {
            "packs/kafkars-broker-role-failover.toml"
        } else if legacy {
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
    assert!(
        pack.scenarios
            .iter()
            .any(|scenario| scenario.ends_with("admin-unregister-broker.toml"))
    );
    let (_, security_pack) =
        match repository.load_pack(Path::new("packs/kafkars-three-broker-security.toml")) {
            Ok(value) => value,
            Err(error) => panic!("load three-broker security pack: {error}"),
        };
    assert!(
        !security_pack
            .scenarios
            .iter()
            .any(|scenario| scenario.ends_with("admin-unregister-broker.toml"))
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

    assert_eq!(pack.scenarios.len(), 125);
    assert!(
        !pack
            .scenarios
            .iter()
            .any(|scenario| scenario.contains("restart")
                || scenario.contains("fencing")
                || scenario.contains("force-termination")
                || scenario.contains("partition-abort")
                || scenario.contains("unregister-broker"))
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
            "producer-explicit-timestamp.toml",
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
        if path != "packs/kafkars-pr.toml" {
            for scenario in [
                "transaction-fencing.toml",
                "transaction-admin-force-termination.toml",
                "transaction-admin-partition-abort.toml",
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
}
