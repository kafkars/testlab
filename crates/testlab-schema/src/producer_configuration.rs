//! Producer configuration models portable client-wide policy without durability downgrades.

use serde::{Deserialize, Serialize};

use crate::ClientId;

/// Public Kafka record-batch compression selection.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProducerCompression {
    /// Leaves record batches uncompressed.
    None,
    /// Uses Kafka-compatible gzip framing.
    Gzip,
    /// Uses Kafka xerial snappy framing.
    Snappy,
    /// Uses Kafka-compatible LZ4 framing.
    Lz4,
    /// Uses Kafka-compatible zstd framing.
    Zstd,
}

/// Portable active, waiting, batching, and request ownership limits.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProducerLimitsConfiguration {
    /// Maximum active application bytes.
    pub retained_bytes: u64,
    /// Maximum active record completions.
    pub in_flight_records: u32,
    /// Maximum waiting callers.
    pub waiting_records: u32,
    /// Maximum bytes retained for waiting callers.
    pub waiting_bytes: u64,
    /// Maximum records accumulated per partition batch.
    pub batch_records: u32,
    /// Maximum encoded bytes per partition batch.
    pub batch_bytes: u64,
    /// Maximum encoded record bytes per Produce request.
    pub request_bytes: u64,
    /// Idempotent request concurrency per broker.
    pub max_in_flight_requests_per_broker: u8,
    /// Maximum engine-owned batching delay.
    pub linger_ms: u64,
}

/// Complete client-wide public producer policy.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProducerConfiguration {
    /// End-to-end duration inherited by producer handles.
    pub delivery_timeout_ms: u64,
    /// Record-batch compression policy.
    pub compression: ProducerCompression,
    /// Maximum safe replacement attempts.
    pub max_retries: u32,
    /// Fixed safe replacement delay.
    pub retry_backoff_ms: u64,
    /// Active, waiting, batching, and request ownership limits.
    pub limits: ProducerLimitsConfiguration,
}

/// Creates one client with an explicit public producer policy.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CreateConfiguredClientAction {
    /// New client identity.
    pub client_id: ClientId,
    /// Producer policy fixed before client startup.
    pub configuration: ProducerConfiguration,
}
