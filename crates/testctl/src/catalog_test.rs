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
    assert_eq!(summary.scenarios, 19);
    assert_eq!(summary.packs, 5);
    assert_eq!(summary.subjects, 2);
    assert_eq!(summary.environments, 17);
    assert_eq!(summary.qualifications, 3);
    assert_eq!(summary.contracts, 39);
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
        let three_broker = cell.environment.contains("/three-");
        let expected = if legacy {
            "packs/kafkars-classic.toml"
        } else if three_broker {
            "packs/kafkars-three-broker.toml"
        } else {
            "packs/kafkars-release.toml"
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
    let (_, pack) = match repository.load_pack(Path::new("packs/kafkars-three-broker.toml")) {
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

    assert_eq!(pack.scenarios.len(), 9);
    assert!(
        !pack
            .scenarios
            .iter()
            .any(|scenario| scenario.contains("restart") || scenario.contains("fencing"))
    );
}
