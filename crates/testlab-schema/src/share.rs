//! Share-group protocol values preserve disposition, delivery count, and close certainty.

use serde::{Deserialize, Serialize};

use crate::ConsumedRecord;

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
