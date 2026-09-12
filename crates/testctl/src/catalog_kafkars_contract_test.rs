//! Kafkars packs claim only semantics supported by the packaged public surface.

use std::{collections::BTreeSet, fs, path::Path, path::PathBuf};

use crate::catalog::Repository;

const INDEPENDENT_HANDLE_SCENARIOS: [&str; 3] = [
    "producer-sibling-close-isolation.toml",
    "producer-replacement-after-close.toml",
    "assigned-consumer-independent-cursors.toml",
];

#[test]
fn kafkars_pr_and_release_packs_qualify_independent_handles() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let repository = Repository::open(&root)
        .unwrap_or_else(|error| panic!("failed to open test repository: {error}"));
    for path in ["packs/kafkars-pr.toml", "packs/kafkars-release.toml"] {
        let (_, pack) = repository
            .load_pack(Path::new(path))
            .unwrap_or_else(|error| panic!("load {path}: {error}"));
        for scenario in INDEPENDENT_HANDLE_SCENARIOS {
            assert!(
                pack.scenarios
                    .iter()
                    .any(|candidate| candidate.ends_with(scenario)),
                "{path} does not qualify {scenario}"
            );
        }
    }
}

#[test]
fn every_kafka_scenario_is_packed() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let repository = Repository::open(&root)
        .unwrap_or_else(|error| panic!("failed to open test repository: {error}"));
    let mut packed = BTreeSet::new();
    for path in kafkars_pack_paths(&root) {
        let (_, pack) = repository
            .load_pack(&path)
            .unwrap_or_else(|error| panic!("load {}: {error}", path.display()));
        for scenario in pack.scenarios {
            let name = Path::new(&scenario)
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or_else(|| {
                    panic!("invalid scenario path in {}: {scenario}", path.display())
                });
            packed.insert(name.to_owned());
        }
    }

    let scenarios = directory_manifest_names(&root.join("scenarios/kafka"));
    let uncovered = scenarios
        .difference(&packed)
        .cloned()
        .collect::<BTreeSet<_>>();

    assert!(
        uncovered.is_empty(),
        "every checked-in Kafka scenario must be packed: {uncovered:?}"
    );
}

fn kafkars_pack_paths(root: &Path) -> Vec<PathBuf> {
    let mut paths = Vec::new();
    let entries = fs::read_dir(root.join("packs"))
        .unwrap_or_else(|error| panic!("list pack directory: {error}"));
    for entry in entries {
        let entry = entry.unwrap_or_else(|error| panic!("read pack directory entry: {error}"));
        let name = entry.file_name();
        let text = name.to_string_lossy();
        if text.starts_with("kafkars-") && text.ends_with(".toml") {
            paths.push(Path::new("packs").join(name));
        }
    }
    paths.sort();
    assert!(!paths.is_empty(), "no Kafkars packs found");
    paths
}

fn directory_manifest_names(directory: &Path) -> BTreeSet<String> {
    fs::read_dir(directory)
        .unwrap_or_else(|error| panic!("list {}: {error}", directory.display()))
        .map(|entry| {
            entry
                .unwrap_or_else(|error| panic!("read {} entry: {error}", directory.display()))
                .file_name()
                .to_string_lossy()
                .into_owned()
        })
        .filter(|name| name.ends_with(".toml"))
        .collect()
}
