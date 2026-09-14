//! Share-capable Kafkars packs retain Share-group Admin lifecycle proofs.

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
                .any(|scenario| scenario.ends_with("admin-describe-share-groups.toml")),
            "{path} omitted caller-ordered plural Share-group descriptions"
        );
        assert!(
            pack.scenarios
                .iter()
                .any(|scenario| scenario.ends_with("admin-list-share-group-offsets.toml")),
            "{path} omitted Share-group offsets"
        );
        assert!(
            pack.scenarios
                .iter()
                .any(|scenario| scenario.ends_with("admin-list-share-groups-offsets.toml")),
            "{path} omitted caller-ordered plural Share-group offsets"
        );
        assert!(
            pack.scenarios
                .iter()
                .any(|scenario| scenario.ends_with("admin-alter-share-group-offsets.toml")),
            "{path} omitted Share-group offset alteration"
        );
        assert!(
            pack.scenarios
                .iter()
                .any(|scenario| scenario.ends_with("admin-delete-share-group-offsets.toml")),
            "{path} omitted Share-group offset deletion"
        );
        assert!(
            pack.scenarios
                .iter()
                .any(|scenario| scenario.ends_with("admin-delete-share-groups.toml")),
            "{path} omitted Share-group deletion"
        );
    }
}

#[test]
fn kafka_4_1_pack_excludes_preview_share_offset_administration() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let repository =
        Repository::open(&root).unwrap_or_else(|error| panic!("open test repository: {error}"));
    let (_, pack) = repository
        .load_pack(Path::new("packs/kafkars-share-4-1.toml"))
        .unwrap_or_else(|error| panic!("load Kafka 4.1 Share pack: {error}"));

    assert!(
        pack.scenarios
            .iter()
            .any(|scenario| scenario.ends_with("admin-describe-share-group.toml"))
    );
    for preview_scenario in [
        "admin-list-share-group-offsets.toml",
        "admin-list-share-groups-offsets.toml",
        "admin-alter-share-group-offsets.toml",
        "admin-delete-share-group-offsets.toml",
    ] {
        assert!(
            !pack
                .scenarios
                .iter()
                .any(|scenario| scenario.ends_with(preview_scenario)),
            "Kafka 4.1 pack retained preview scenario {preview_scenario}"
        );
    }
}
