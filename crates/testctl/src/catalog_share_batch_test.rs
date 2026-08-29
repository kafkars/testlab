//! Share batch catalog tests retain mixed decisions in every capable Kafkars pack.

use std::path::Path;

use crate::catalog::Repository;

#[test]
fn share_capable_packs_retain_batch_decisions() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let repository = Repository::open(&root)
        .unwrap_or_else(|error| panic!("failed to open test repository: {error}"));
    for path in [
        "packs/kafkars-pr.toml",
        "packs/kafkars-share-release.toml",
        "packs/kafkars-three-broker-share.toml",
        "packs/kafkars-three-broker-security.toml",
        "packs/kafkars-share-batches.toml",
    ] {
        let (_, pack) = repository
            .load_pack(Path::new(path))
            .unwrap_or_else(|error| panic!("load {path}: {error}"));
        for scenario in [
            "share-group-mixed-release.toml",
            "share-group-mixed-reject.toml",
            "share-group-batch-drop.toml",
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
