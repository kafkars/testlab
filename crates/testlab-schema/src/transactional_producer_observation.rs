//! Public transactional producer observations retain initialized owner identity and state.

use serde::{Deserialize, Serialize};

use crate::ProducerId;

/// Exact values read from one successfully initialized public transactional producer.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TransactionalProducerObservation {
    /// Scenario-local handle identity carried by the creation command.
    pub producer_id: ProducerId,
    /// Public `TransactionalProducer::transactional_id` result.
    pub transactional_id: String,
    /// Public broker-issued producer identity.
    pub kafka_producer_id: i64,
    /// Public broker-issued producer epoch.
    pub kafka_producer_epoch: i16,
    /// Public `TransactionalProducer::is_active` result after initialization.
    pub active: bool,
}

#[cfg(test)]
mod tests {
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
}
