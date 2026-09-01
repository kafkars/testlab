//! Lifecycle catalog tests keep supported ownership boundaries in every Kafkars pack.

use std::path::Path;

use crate::catalog::Repository;

#[test]
fn kafkars_pack_variants_retain_supported_lifecycle_coverage() {
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
        "packs/kafkars-lifecycle-isolation.toml",
    ] {
        let (_, pack) = match repository.load_pack(Path::new(path)) {
            Ok(value) => value,
            Err(error) => panic!("load {path}: {error}"),
        };
        for scenario in [
            "producer-repeated-readiness-flush.toml",
            "client-shutdown-isolation.toml",
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
