//! Concurrent provisioning tests derive every actor send topic and partition externally.

use std::collections::BTreeMap;

use testlab_schema::Scenario;

use crate::compose_provision_targets::topics;

#[test]
fn concurrent_send_topics_are_preprovisioned_at_the_highest_partition() {
    let scenario: Scenario = toml::from_str(include_str!(
        "../../../scenarios/kafka/concurrent-multi-producer.toml"
    ))
    .unwrap_or_else(|error| panic!("parse concurrent scenario: {error}"));

    assert_eq!(
        topics(&scenario),
        BTreeMap::from([("testlab-kafkars-concurrent-multi-producer".to_owned(), 2,)])
    );
}
