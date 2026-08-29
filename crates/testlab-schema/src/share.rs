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
    /// Soft record ceiling requested from Kafka for one `ShareFetch`.
    pub max_records: u32,
    /// Preferred record count for each acquired range.
    pub batch_size: u32,
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

/// One exact record acquired through the packaged share-consumer API.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ShareConsumedRecord {
    /// Exact public record bytes and Kafka coordinates.
    pub record: ConsumedRecord,
    /// Positive broker-reported share delivery count.
    pub delivery_count: i16,
}
