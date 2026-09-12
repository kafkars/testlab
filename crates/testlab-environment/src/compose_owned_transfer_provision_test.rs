//! Owned transfer provisioning includes both source and destination partitions.

use std::collections::BTreeMap;

use testlab_schema::Scenario;

use crate::compose_provision_targets::topics;

#[test]
fn owned_transfer_destination_is_preprovisioned() {
    let scenario: Scenario = toml::from_str(include_str!(
        "../../../scenarios/kafka/assigned-consumer-owned-record-transfer.toml"
    ))
    .unwrap_or_else(|error| panic!("parse owned transfer scenario: {error}"));
    assert_eq!(
        topics(&scenario),
        BTreeMap::from([
            ("testlab-kafkars-owned-transfer-destination".to_owned(), 2),
            ("testlab-kafkars-owned-transfer-source".to_owned(), 1),
        ])
    );
}
