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
    /// Bounded FIFO admission through the public producer `send` method.
    ProducerWaitingSend,
    /// Stage-aware public producer cancellation.
    ProducerCancellation,
    /// Client-wide public producer configuration.
    ProducerConfiguration,
    /// Public batch-producer operations.
    ProducerBatch,
    /// Harness-scheduled concurrent packaged-client actors.
    ConcurrentActors,
    /// Explicit close, shutdown, and finish behavior.
    Lifecycle,
    /// Private execution and lifecycle ownership for public child handles.
    IndependentHandles,
    /// Explicit public client readiness probing.
    ClientReadiness,
    /// Startup and readiness enforcement of one exact broker cluster identity.
    ExpectedClusterIdentity,
    /// Bounded public client operational metrics snapshots.
    ClientMetrics,
    /// Testlab's self-test model broker transport.
    ModelBroker,
    /// Assigned-partition consumer operations.
    AssignedConsumer,
    /// Immediate retained-batch observation through `try_take_batch`.
    AssignedConsumerImmediateBatch,
    /// Waiting and immediate retained-failure event observation.
    AssignedConsumerEvents,
    /// Client-wide immutable assigned-consumer configuration.
    AssignedConsumerConfiguration,
    /// Positioning and mutation controls for assigned-partition consumers.
    AssignedConsumerControls,
    /// Consumer group operations.
    ConsumerGroups,
    /// KIP-848 consumer group operations.
    ConsumerProtocolGroups,
    /// Immediate retained-batch observation through hosted `try_take_batch`.
    GroupConsumerImmediateBatch,
    /// Public application-processing liveness acknowledgement.
    GroupConsumerAcknowledge,
    /// Ordered processed-prefix checkpoint commits.
    GroupConsumerPartialCheckpoint,
    /// Runtime pause, resume, and seek controls for hosted group consumers.
    GroupConsumerControls,
    /// Missing-offset, visibility, static identity, and classic timing configuration.
    GroupConsumerConfiguration,
    /// Clone-shared shutdown and public event-stream termination for hosted groups.
    GroupConsumerShutdown,
    /// KIP-932 share-group acquisition and acknowledgement operations.
    ShareConsumer,
    /// Immutable public `ShareFetch` record and acquisition-range policy.
    ShareConsumerConfiguration,
    /// Transactional producer operations.
    Transactions,
    /// Homogeneous public transactional batch staging.
    TransactionBatchSend,
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
