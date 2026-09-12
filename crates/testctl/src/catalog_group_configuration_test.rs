//! Group-configuration catalog tests preserve both protocol variants in compatible packs.

use std::path::Path;

use crate::catalog::Repository;

#[test]
fn capable_kafkars_packs_retain_group_configuration() {
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
        "packs/kafkars-group-configuration.toml",
    ] {
        assert_scenarios(&repository, path, &ALL_SCENARIOS);
    }
    assert_scenarios(
        &repository,
        "packs/kafkars-classic.toml",
        &CLASSIC_SCENARIOS,
    );
}

const ALL_SCENARIOS: [&str; 9] = [
    "admin-remove-static-group-members.toml",
    "classic-group-latest-offset-reset.toml",
    "consumer-protocol-group-latest-offset-reset.toml",
    "classic-group-multi-topic-subscription.toml",
    "consumer-protocol-group-multi-topic-subscription.toml",
    "classic-group-read-committed.toml",
    "consumer-protocol-group-read-committed.toml",
    "classic-group-seek-replay.toml",
    "consumer-protocol-group-seek-replay.toml",
];

const CLASSIC_SCENARIOS: [&str; 5] = [
    "admin-remove-static-group-members.toml",
    "classic-group-latest-offset-reset.toml",
    "classic-group-multi-topic-subscription.toml",
    "classic-group-read-committed.toml",
    "classic-group-seek-replay.toml",
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
