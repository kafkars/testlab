//! Share-capable Kafkars packs retain Share-group description and offset proofs.

use std::path::Path;

use crate::catalog::Repository;

#[test]
fn share_packs_retain_public_and_independent_group_admin_proofs() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let repository =
        Repository::open(&root).unwrap_or_else(|error| panic!("open test repository: {error}"));
    for path in [
        "packs/kafkars-pr.toml",
        "packs/kafkars-share-release.toml",
        "packs/kafkars-three-broker-share.toml",
    ] {
        let (_, pack) = repository
            .load_pack(Path::new(path))
            .unwrap_or_else(|error| panic!("load {path}: {error}"));
        assert!(
            pack.scenarios
                .iter()
                .any(|scenario| { scenario.ends_with("admin-describe-share-group.toml") }),
            "{path} omitted active Share-group description"
        );
        assert!(
            pack.scenarios
                .iter()
                .any(|scenario| scenario.ends_with("admin-list-share-group-offsets.toml")),
            "{path} omitted Share-group offsets"
        );
    }
}
