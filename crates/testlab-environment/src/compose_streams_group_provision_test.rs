//! Tests for the compose streams group provision contract.

use testlab_schema::Scenario;

use super::{SCRIPT, fixture};

#[test]
fn checked_in_scenario_selects_two_real_streams_groups() {
    let scenario: Scenario = toml::from_str(include_str!(
        "../../../scenarios/kafka/admin-streams-group-lifecycle.toml"
    ))
    .unwrap_or_else(|error| panic!("parse Streams-group scenario: {error}"));
    let fixture = fixture(&scenario)
        .unwrap_or_else(|error| panic!("fixture: {error}"))
        .unwrap_or_else(|| panic!("missing fixture"));
    assert_eq!(
        fixture.group_ids,
        ["testlab-streams-primary", "testlab-streams-secondary"]
    );
    assert!(SCRIPT.contains("group.protocol=streams"));
    assert!(SCRIPT.contains("WordCountDemo"));
    assert!(SCRIPT.contains("BuiltInDslStoreSuppliers$InMemoryDslStoreSuppliers"));
    assert!(SCRIPT.contains("kafka-streams-groups.sh"));
    assert!(SCRIPT.contains("jobs -pr"));
    assert!(SCRIPT.contains("exited with status $status"));
}
