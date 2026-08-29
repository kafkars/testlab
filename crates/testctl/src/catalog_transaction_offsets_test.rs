//! Transactional offset catalog tests retain both public group protocols.

use std::path::Path;

use crate::catalog::Repository;

#[test]
fn compatible_packs_retain_transactional_offset_atomicity() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let repository = Repository::open(&root)
        .unwrap_or_else(|error| panic!("failed to open test repository: {error}"));
    for path in [
        "packs/kafkars-pr.toml",
        "packs/kafkars-release.toml",
        "packs/kafkars-share-release.toml",
        "packs/kafkars-three-broker.toml",
        "packs/kafkars-three-broker-security.toml",
        "packs/kafkars-three-broker-share.toml",
        "packs/kafkars-transactions.toml",
        "packs/kafkars-transactional-offsets.toml",
    ] {
        let (_, pack) = repository
            .load_pack(Path::new(path))
            .unwrap_or_else(|error| panic!("load {path}: {error}"));
        for scenario in [
            "transactional-offset-classic.toml",
            "transactional-offset-consumer.toml",
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
fn legacy_pack_retains_only_classic_transactional_offsets() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let repository = Repository::open(&root)
        .unwrap_or_else(|error| panic!("failed to open test repository: {error}"));
    let (_, pack) = repository
        .load_pack(Path::new("packs/kafkars-classic.toml"))
        .unwrap_or_else(|error| panic!("load classic pack: {error}"));
    assert!(
        pack.scenarios
            .iter()
            .any(|scenario| scenario.ends_with("transactional-offset-classic.toml"))
    );
    assert!(
        !pack
            .scenarios
            .iter()
            .any(|scenario| scenario.ends_with("transactional-offset-consumer.toml"))
    );
}
