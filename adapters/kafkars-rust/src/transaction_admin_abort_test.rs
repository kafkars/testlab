//! Administrative transaction-abort tests pin exact producer identities.

use testlab_schema::ProducerStateSnapshot;

use crate::transaction_admin_abort::abort_spec;

#[test]
fn abort_spec_uses_exact_public_producer_identity() {
    let spec = abort_spec(&[producer(Some(17))], "orders", 2)
        .unwrap_or_else(|error| panic!("abort spec: {error}"));
    assert_eq!(spec.topic_partition().topic(), "orders");
    assert_eq!(spec.topic_partition().partition(), 2);
    assert_eq!(spec.producer_id(), 71);
    assert_eq!(spec.producer_epoch(), 3);
    assert_eq!(spec.coordinator_epoch(), 4);
    assert_eq!(spec.requested_transaction_version(), 0);
}

#[test]
fn abort_spec_requires_one_open_transaction() {
    assert!(abort_spec(&[], "orders", 2).is_err());
    assert!(abort_spec(&[producer(None)], "orders", 2).is_err());
    assert!(abort_spec(&[producer(Some(17)), producer(Some(18))], "orders", 2).is_err());
}

fn producer(start: Option<i64>) -> ProducerStateSnapshot {
    ProducerStateSnapshot {
        producer_id: 71,
        producer_epoch: 3,
        last_sequence: 0,
        last_timestamp: 1_700_000_000_000,
        coordinator_epoch: 4,
        current_transaction_start_offset: start,
    }
}
