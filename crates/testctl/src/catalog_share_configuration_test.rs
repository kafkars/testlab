//! Share-configuration catalog tests preserve supported Kafkars pack coverage.

use std::path::Path;

use crate::catalog::Repository;

#[test]
fn share_capable_kafkars_packs_retain_max_records_configuration() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let repository = Repository::open(&root)
        .unwrap_or_else(|error| panic!("failed to open test repository: {error}"));
    for path in [
        "packs/kafkars-pr.toml",
        "packs/kafkars-share-release.toml",
        "packs/kafkars-three-broker-security.toml",
        "packs/kafkars-three-broker-share.toml",
        "packs/kafkars-share-configuration.toml",
    ] {
        let (_, pack) = repository
            .load_pack(Path::new(path))
            .unwrap_or_else(|error| panic!("load {path}: {error}"));
        assert!(
            pack.scenarios
                .iter()
                .any(|candidate| candidate.ends_with(SCENARIO)),
            "{path} omitted {SCENARIO}"
        );
    }
}

const SCENARIO: &str = "share-group-fetch-max-records.toml";
