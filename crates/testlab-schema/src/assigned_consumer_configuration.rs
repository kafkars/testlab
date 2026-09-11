//! Assigned-consumer policy is fixed before the owning client host starts.

use serde::{Deserialize, Serialize};

use crate::ClientId;

/// Public transactional visibility selected for a directly assigned consumer.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AssignedConsumerReadIsolation {
    /// Deliver committed, aborted, and unresolved transaction records.
    #[default]
    ReadUncommitted,
    /// Deliver only nontransactional and committed transaction records.
    ReadCommitted,
}

/// Immutable client-wide policy for one directly assigned consumer.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AssignedConsumerConfiguration {
    /// Transactional record visibility.
    pub read_isolation: AssignedConsumerReadIsolation,
}

/// Creates a client with explicit directly assigned consumer policy.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CreateAssignedConsumerClientAction {
    /// New client identity.
    pub client_id: ClientId,
    /// Assigned-consumer policy fixed before client startup.
    pub configuration: AssignedConsumerConfiguration,
}

#[cfg(test)]
#[path = "assigned_consumer_configuration_test.rs"]
mod tests;
