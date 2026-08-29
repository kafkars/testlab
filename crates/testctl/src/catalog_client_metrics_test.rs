//! Client metrics catalog tests retain the scenario in every capable Kafka pack.

use std::path::Path;

use crate::catalog::Repository;

#[test]
fn capable_kafkars_packs_retain_client_metrics() {
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
        "packs/kafkars-lifecycle-isolation.toml",
        "packs/kafkars-client-metrics.toml",
    ] {
        let (_, pack) = repository
            .load_pack(Path::new(path))
            .unwrap_or_else(|error| panic!("load {path}: {error}"));
        assert!(
            pack.scenarios
                .iter()
                .any(|candidate| candidate.ends_with("client-metrics-producer.toml")),
            "{path} omitted client metrics"
        );
    }
}
