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
    assert_eq!(summary.scenarios, 20);
    assert_eq!(summary.packs, 8);
    assert_eq!(summary.subjects, 2);
    assert_eq!(summary.environments, 17);
    assert_eq!(summary.qualifications, 3);
    assert_eq!(summary.contracts, 40);
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

    assert_eq!(pack.scenarios.len(), 7);
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
}
