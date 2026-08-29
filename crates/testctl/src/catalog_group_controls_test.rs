//! Group-control catalog tests retain all protocol variants in capable Kafka packs.

use std::path::Path;

use crate::catalog::Repository;

#[test]
fn capable_kafkars_packs_retain_group_controls() {
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
        "packs/kafkars-group-controls.toml",
    ] {
        let (_, pack) = repository
            .load_pack(Path::new(path))
            .unwrap_or_else(|error| panic!("load {path}: {error}"));
        for scenario in [
            "classic-group-seek-replay.toml",
            "consumer-protocol-group-seek-replay.toml",
            "classic-group-pause-resume.toml",
            "consumer-protocol-group-pause-resume.toml",
        ] {
            assert!(
                pack.scenarios
                    .iter()
                    .any(|candidate| candidate.ends_with(scenario)),
                "{path} omitted {scenario}"
            );
        }
    }

    let (_, classic) = repository
        .load_pack(Path::new("packs/kafkars-classic.toml"))
        .unwrap_or_else(|error| panic!("load classic pack: {error}"));
    for scenario in [
        "classic-group-seek-replay.toml",
        "classic-group-pause-resume.toml",
    ] {
        assert!(
            classic
                .scenarios
                .iter()
                .any(|candidate| candidate.ends_with(scenario)),
            "classic pack omitted {scenario}"
        );
    }
}
