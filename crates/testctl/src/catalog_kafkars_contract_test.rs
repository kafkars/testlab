//! Kafkars packs claim only semantics supported by the packaged public surface.

use std::path::Path;

use crate::catalog::Repository;

#[test]
fn kafkars_packs_exclude_unsupported_handle_and_error_contracts() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let repository = Repository::open(&root)
        .unwrap_or_else(|error| panic!("failed to open test repository: {error}"));
    for path in [
        "packs/kafkars-pr.toml",
        "packs/kafkars-classic.toml",
        "packs/kafkars-release.toml",
        "packs/kafkars-share-release.toml",
        "packs/kafkars-share-configuration.toml",
        "packs/kafkars-three-broker.toml",
        "packs/kafkars-three-broker-share.toml",
        "packs/kafkars-three-broker-security.toml",
    ] {
        let (_, pack) = repository
            .load_pack(Path::new(path))
            .unwrap_or_else(|error| panic!("load {path}: {error}"));
        for scenario in [
            "producer-sibling-close-isolation.toml",
            "producer-replacement-after-close.toml",
            "assigned-consumer-independent-cursors.toml",
            "admin-list-offsets-invalid-partition.toml",
            "share-group-fetch-batch-size.toml",
        ] {
            assert!(
                !pack
                    .scenarios
                    .iter()
                    .any(|candidate| candidate.ends_with(scenario)),
                "{path} claims unsupported {scenario}"
            );
        }
    }
}
