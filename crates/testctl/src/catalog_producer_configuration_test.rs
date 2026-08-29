//! Producer configuration catalog tests retain every codec in capable packs.

use std::path::Path;

use crate::catalog::Repository;

const SCENARIOS: [&str; 5] = [
    "producer-configuration-none.toml",
    "producer-configuration-gzip.toml",
    "producer-configuration-snappy.toml",
    "producer-configuration-lz4.toml",
    "producer-configuration-zstd.toml",
];

#[test]
fn capable_kafkars_packs_retain_every_producer_configuration() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let repository = Repository::open(&root)
        .unwrap_or_else(|error| panic!("failed to open test repository: {error}"));
    for path in [
        "packs/kafkars-pr.toml",
        "packs/kafkars-release.toml",
        "packs/kafkars-share-release.toml",
        "packs/kafkars-classic.toml",
        "packs/kafkars-three-broker.toml",
        "packs/kafkars-three-broker-security.toml",
        "packs/kafkars-three-broker-share.toml",
        "packs/kafkars-producer-configuration.toml",
    ] {
        let (_, pack) = repository
            .load_pack(Path::new(path))
            .unwrap_or_else(|error| panic!("load {path}: {error}"));
        for scenario in SCENARIOS {
            assert!(
                pack.scenarios
                    .iter()
                    .any(|candidate| candidate.ends_with(scenario)),
                "{path} omitted {scenario}"
            );
        }
    }
}
