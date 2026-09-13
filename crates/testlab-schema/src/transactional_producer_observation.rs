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
#[path = "transactional_producer_observation_test.rs"]
mod tests;
