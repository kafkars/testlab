//! Group-shutdown catalog tests preserve classic and KIP-848 qualification placement.

use std::path::Path;

use crate::catalog::Repository;

#[test]
fn capable_kafkars_packs_retain_group_shutdown() {
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
        "packs/kafkars-group-shutdown.toml",
    ] {
        assert_scenarios(&repository, path, &ALL_SCENARIOS);
    }
    assert_scenarios(
        &repository,
        "packs/kafkars-classic.toml",
        &["classic-group-shutdown.toml"],
    );
}

const ALL_SCENARIOS: [&str; 2] = [
    "classic-group-shutdown.toml",
    "consumer-protocol-group-shutdown.toml",
];

fn assert_scenarios(repository: &Repository, path: &str, expected: &[&str]) {
    let (_, pack) = repository
        .load_pack(Path::new(path))
        .unwrap_or_else(|error| panic!("load {path}: {error}"));
    for scenario in expected {
        assert!(
            pack.scenarios
                .iter()
                .any(|candidate| candidate.ends_with(scenario)),
            "{path} omitted {scenario}"
        );
    }
}
