//! Automatic producer partitioning provisions the declared logical topology.

use std::collections::BTreeMap;

use testlab_schema::Scenario;

use crate::compose_provision_targets::topics;

#[test]
fn automatic_keyed_send_uses_declared_logical_partition_count() {
    let scenario: Scenario = toml::from_str(include_str!(
        "../../../scenarios/kafka/producer-automatic-keyed-partition.toml"
    ))
    .unwrap_or_else(|error| panic!("parse automatic partition scenario: {error}"));

    assert_eq!(
        topics(&scenario),
        BTreeMap::from([(
            "testlab-kafkars-producer-automatic-keyed-partition".to_owned(),
            3,
        )])
    );
}
