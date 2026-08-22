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
    assert_eq!(summary.scenarios, 12);
    assert_eq!(summary.packs, 3);
    assert_eq!(summary.subjects, 2);
    assert_eq!(summary.environments, 17);
    assert_eq!(summary.qualifications, 3);
    assert_eq!(summary.contracts, 35);
}

#[test]
fn pre_kip_848_release_cells_use_the_classic_pack() {
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
        assert_eq!(
            cell.pack == "packs/kafkars-classic.toml",
            legacy,
            "unexpected pack for {}",
            cell.id
        );
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
}
