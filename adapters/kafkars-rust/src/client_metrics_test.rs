//! Client metrics tests exercise the packaged public snapshot without broker internals.

use testlab_schema::{AdapterSecurity, ClientId, OperationId};

use crate::state::AdapterState;

#[test]
fn idle_client_normalizes_every_public_metrics_family() {
    let client_id =
        ClientId::new("metrics-client").unwrap_or_else(|error| panic!("client id: {error}"));
    let operation_id = OperationId::new("metrics-snapshot")
        .unwrap_or_else(|error| panic!("operation id: {error}"));
    let mut state = AdapterState::default();
    state
        .hello(vec!["127.0.0.1:1".to_owned()], AdapterSecurity::Plaintext)
        .unwrap_or_else(|error| panic!("hello: {error}"));
    state
        .create_client(client_id.clone())
        .unwrap_or_else(|error| panic!("create client: {error}"));

    let observation = state
        .observe_client_metrics(client_id.clone(), operation_id.clone())
        .unwrap_or_else(|error| panic!("observe metrics: {error}"));
    assert_eq!(observation.client_id, client_id);
    assert_eq!(observation.operation_id, operation_id);
    assert_eq!(observation.snapshot.calls.admitted, 0);
    assert_eq!(observation.snapshot.failures.deadline, 0);
    assert!(observation.snapshot.mailbox.capacity_per_lane > 0);
    assert_eq!(observation.snapshot.latency.end_to_end.samples, 0);
    assert_eq!(observation.snapshot.producer.produce_records, 0);
    assert!(observation.snapshot.producer.accepting);
    assert!(observation.snapshot.producer.healthy);

    state
        .shutdown_client(&client_id)
        .unwrap_or_else(|error| panic!("shutdown client: {error}"));
}
