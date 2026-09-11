//! Feature-update validation remains limited to exact Kafka 4.3.1 packs.

use std::path::Path;

use crate::catalog::Repository;

#[test]
fn kafka_4_3_packs_retain_validation_only_feature_update() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let repository = Repository::open(&root)
        .unwrap_or_else(|error| panic!("failed to open test repository: {error}"));
    for path in [
        "packs/kafkars-pr.toml",
        "packs/kafkars-three-broker.toml",
        "packs/kafkars-three-broker-security.toml",
        "packs/kafkars-three-broker-share.toml",
    ] {
        let (_, pack) = repository
            .load_pack(Path::new(path))
            .unwrap_or_else(|error| panic!("load {path}: {error}"));
        assert!(
            pack.scenarios
                .iter()
                .any(|candidate| candidate.ends_with("admin-validate-feature-updates.toml")),
            "{path} omitted validation-only feature update"
        );
    }
    for path in [
        "packs/kafkars-classic.toml",
        "packs/kafkars-release.toml",
        "packs/kafkars-share-release.toml",
    ] {
        let (_, pack) = repository
            .load_pack(Path::new(path))
            .unwrap_or_else(|error| panic!("load {path}: {error}"));
        assert!(
            !pack
                .scenarios
                .iter()
                .any(|candidate| candidate.ends_with("admin-validate-feature-updates.toml")),
            "{path} widened a Kafka 4.3.1-only contract"
        );
    }
}
