//! Modern Streams-group Admin coverage stays isolated to its Kafka 4.3.1 cell.

use std::path::Path;

use crate::catalog::Repository;

#[test]
fn release_retains_exact_streams_group_admin_cell() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let repository = Repository::open(&root)
        .unwrap_or_else(|error| panic!("failed to open test repository: {error}"));
    let (_, qualification) = repository
        .load_qualification(Path::new("qualifications/kafkars-release.toml"))
        .unwrap_or_else(|error| panic!("load release qualification: {error}"));
    let cell = qualification
        .cells
        .iter()
        .find(|cell| cell.id.as_str() == "apache-kafka-4-3-1-streams-group-admin")
        .unwrap_or_else(|| panic!("release qualification omitted Streams-group Admin"));
    assert_eq!(
        cell.environment,
        "clusters/apache-kafka/4.3.1/three-plaintext.toml"
    );
    assert_eq!(cell.pack, "packs/kafkars-streams-group.toml");
    assert_eq!(cell.attempts, 1);
    assert!(cell.gating);
    let (_, pack) = repository
        .load_pack(Path::new(&cell.pack))
        .unwrap_or_else(|error| panic!("load Streams-group pack: {error}"));
    assert_eq!(
        pack.scenarios,
        ["scenarios/kafka/admin-streams-group-lifecycle.toml"]
    );
    assert!(
        include_str!("../../../clusters/apache-kafka/compose/three-share.yml")
            .contains("classic,consumer,share,streams")
    );
}
