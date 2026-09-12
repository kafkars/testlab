//! Share-group protocol values preserve disposition, delivery count, and close certainty.

use serde::{Deserialize, Serialize};

use crate::ConsumedRecord;

#[cfg(test)]
#[path = "share_configuration_test.rs"]
mod share_configuration_test;

/// Portable `ShareFetch` acquisition policy fixed before membership starts.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ShareConsumerFetchConfiguration {
    /// Broker maximum long-poll interval.
    pub max_wait_ms: u64,
    /// Preferred minimum response bytes.
    pub min_bytes: u64,
    /// Soft whole-response byte ceiling.
    pub max_bytes: u64,
    /// Soft record ceiling requested from Kafka for one `ShareFetch`.
    pub max_records: u32,
    /// Preferred record count for each acquired range.
    pub batch_size: u32,
    /// End-to-end deadline for each background `ShareFetch` attempt.
    pub attempt_timeout_ms: u64,
}

/// Application outcome for one acquired share record.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ShareDisposition {
    /// Processing succeeded and normal redelivery must stop.
    Accept,
    /// Processing may succeed later and the record should become available.
    Release,
    /// Processing is permanently rejected and normal redelivery must stop.
    Reject,
}

/// Public conversion selected for one retained Share batch.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ShareAcknowledgementMethod {
    /// Supplies one explicit disposition for every retained record.
    #[default]
    IntoAcknowledgement,
    /// Uses the batch's all-record Accept convenience path.
    AcceptAll,
}

/// One exact record acquired through the packaged share-consumer API.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ShareConsumedRecord {
    /// Exact public record bytes and Kafka coordinates.
    pub record: ConsumedRecord,
    /// Positive broker-reported share delivery count.
    pub delivery_count: i16,
}
