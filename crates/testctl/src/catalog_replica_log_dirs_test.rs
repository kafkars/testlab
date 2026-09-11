//! Multi-directory replica movement stays in every three-broker Kafkars pack.

use std::path::Path;

use crate::catalog::Repository;

#[test]
fn three_broker_packs_retain_replica_log_directory_alteration() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let repository = Repository::open(&root)
        .unwrap_or_else(|error| panic!("failed to open test repository: {error}"));
    for path in [
        "packs/kafkars-three-broker.toml",
        "packs/kafkars-three-broker-share.toml",
        "packs/kafkars-three-broker-security.toml",
    ] {
        let (_, pack) = repository
            .load_pack(Path::new(path))
            .unwrap_or_else(|error| panic!("load {path}: {error}"));
        assert!(
            pack.scenarios
                .iter()
                .any(|scenario| scenario.ends_with("admin-alter-replica-log-dirs.toml")),
            "{path} omitted replica log-directory alteration"
        );
    }
}
