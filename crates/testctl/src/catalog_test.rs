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
    assert_eq!(summary.scenarios, 4);
    assert_eq!(summary.packs, 2);
    assert_eq!(summary.subjects, 2);
    assert_eq!(summary.environments, 17);
    assert_eq!(summary.qualifications, 3);
    assert_eq!(summary.contracts, 20);
}
