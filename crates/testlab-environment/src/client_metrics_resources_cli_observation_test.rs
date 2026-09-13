//! Tests for the client metrics resources cli observation contract.

use testlab_schema::{BrokerStateObservation, OperationId};

#[test]
fn normalizes_names_in_utf8_byte_order() {
    let state = super::normalize(7, &operation(), b"metrics-z\nmetrics-a\n")
        .unwrap_or_else(|error| panic!("normalize: {error}"));
    let BrokerStateObservation::ConfigResources(state) = state else {
        panic!("configuration-resource state");
    };
    assert_eq!(state.observation, 7);
    assert_eq!(state.operation_id, operation());
    assert_eq!(
        state
            .resources
            .iter()
            .map(|resource| (resource.resource_type, resource.name.as_str()))
            .collect::<Vec<_>>(),
        vec![(16, "metrics-a"), (16, "metrics-z")]
    );
}

#[test]
fn accepts_empty_output_and_rejects_ambiguous_names() {
    let empty = super::normalize(1, &operation(), b"\n")
        .unwrap_or_else(|error| panic!("empty listing: {error}"));
    let BrokerStateObservation::ConfigResources(empty) = empty else {
        panic!("configuration-resource state");
    };
    assert!(empty.resources.is_empty());
    assert!(super::normalize(1, &operation(), b"metrics-a\nmetrics-a\n").is_err());
    assert!(super::normalize(1, &operation(), b" metrics-a\n").is_err());
    assert!(super::normalize(1, &operation(), b"metrics\ta\n").is_err());
}

fn operation() -> OperationId {
    OperationId::new("admin-list-client-metrics-resources")
        .unwrap_or_else(|error| panic!("operation: {error}"))
}
