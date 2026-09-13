//! Tests for the transactional producer observation contract.

use super::TransactionalProducerObservation;
use crate::{AdapterEvent, ProducerId};

#[test]
fn created_event_flattens_the_complete_public_observation() {
    let event = AdapterEvent::TransactionalProducerCreated(TransactionalProducerObservation {
        producer_id: ProducerId::new("transactional-1")
            .unwrap_or_else(|error| panic!("producer ID: {error}")),
        transactional_id: "testlab-transaction".to_owned(),
        kafka_producer_id: 41,
        kafka_producer_epoch: 2,
        active: true,
    });
    let value = serde_json::to_value(&event)
        .unwrap_or_else(|error| panic!("serialize transactional producer: {error}"));
    assert_eq!(
        value,
        serde_json::json!({
            "kind": "transactional_producer_created",
            "producer_id": "transactional-1",
            "transactional_id": "testlab-transaction",
            "kafka_producer_id": 41,
            "kafka_producer_epoch": 2,
            "active": true,
        })
    );
    let decoded: AdapterEvent = serde_json::from_value(value)
        .unwrap_or_else(|error| panic!("deserialize transactional producer: {error}"));
    assert_eq!(decoded, event);
}
