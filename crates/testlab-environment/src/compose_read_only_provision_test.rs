//! Read-only admin provisioning creates only scenario-owned external topics.

use std::collections::BTreeMap;

use testlab_schema::Scenario;

#[test]
fn read_only_admin_topics_are_preprovisioned_from_their_markers() {
    for (path, expected) in [
        (
            "../../scenarios/kafka/admin-describe-topic.toml",
            BTreeMap::from([("testlab-kafkars-admin-described".to_owned(), 3)]),
        ),
        (
            "../../scenarios/kafka/admin-list-topics.toml",
            BTreeMap::from([("testlab-kafkars-admin-listed".to_owned(), 1)]),
        ),
        (
            "../../scenarios/kafka/admin-list-internal-topics.toml",
            BTreeMap::from([("testlab-kafkars-admin-internal-topic".to_owned(), 1)]),
        ),
        (
            "../../scenarios/kafka/admin-list-offsets.toml",
            BTreeMap::from([("testlab-kafkars-admin-offsets".to_owned(), 1)]),
        ),
        (
            "../../scenarios/kafka/admin-list-consumer-group-offsets.toml",
            BTreeMap::from([("testlab-kafkars-admin-group-offsets".to_owned(), 1)]),
        ),
        (
            "../../scenarios/kafka/admin-topic-config-lifecycle.toml",
            BTreeMap::from([("testlab-kafkars-admin-topic-config".to_owned(), 1)]),
        ),
    ] {
        let manifest =
            std::fs::read_to_string(std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(path))
                .unwrap_or_else(|error| panic!("read {path}: {error}"));
        let scenario: Scenario =
            toml::from_str(&manifest).unwrap_or_else(|error| panic!("parse {path}: {error}"));

        assert_eq!(
            super::topics(&scenario),
            expected,
            "unexpected topics for {path}"
        );
    }
}
