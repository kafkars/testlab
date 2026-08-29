//! Assigned-control catalog tests retain every scenario in each capable Kafka pack.

use std::path::Path;

use crate::catalog::Repository;

#[test]
fn capable_kafkars_packs_retain_assigned_controls() {
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
        "packs/kafkars-assigned-consumer-cursors.toml",
    ] {
        let (_, pack) = repository
            .load_pack(Path::new(path))
            .unwrap_or_else(|error| panic!("load {path}: {error}"));
        for scenario in [
            "assigned-consumer-end-position.toml",
            "assigned-consumer-exact-offset.toml",
            "assigned-consumer-seek-replay.toml",
            "assigned-consumer-pause-resume.toml",
            "assigned-consumer-incremental-assignments.toml",
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
