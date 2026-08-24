//! Adapter identity and capability declarations gate scenario eligibility.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::AdapterId;

/// One black-box capability exposed by an adapter.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Capability {
    /// Public producer operations.
    Producer,
    /// Public batch-producer operations.
    ProducerBatch,
    /// Explicit close, shutdown, and finish behavior.
    Lifecycle,
    /// Explicit public client readiness probing.
    ClientReadiness,
    /// Testlab's self-test model broker transport.
    ModelBroker,
    /// Assigned-partition consumer operations.
    AssignedConsumer,
    /// Consumer group operations.
    ConsumerGroups,
    /// KIP-848 consumer group operations.
    ConsumerProtocolGroups,
    /// KIP-932 share-group acquisition and acknowledgement operations.
    ShareConsumer,
    /// Transactional producer operations.
    Transactions,
    /// Administrative operations.
    Admin,
    /// TLS transport.
    Tls,
    /// SASL/SCRAM authentication.
    Scram,
    /// Foreign-function interface surface.
    Ffi,
}

/// Adapter identity returned during the protocol handshake.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdapterDescriptor {
    /// Stable adapter identity.
    pub id: AdapterId,
    /// Human-readable implementation name.
    pub implementation: String,
    /// Adapter or packaged client version.
    pub version: String,
    /// Exact control protocol version.
    pub protocol_version: u16,
    /// Supported black-box capabilities.
    pub capabilities: BTreeSet<Capability>,
}
