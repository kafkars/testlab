//! Watermark acquisition retains the independent observer's original deadline.

use std::time::{Duration, Instant};

use rdkafka::error::KafkaError;
use rdkafka::types::RDKafkaErrorCode;

use crate::observer::remaining;
use crate::observer_error::ObserverError;

const RETRY_DELAY: Duration = Duration::from_millis(50);

pub(super) fn capture(
    deadline: Instant,
    mut query: impl FnMut(Duration) -> Result<(i64, i64), KafkaError>,
) -> Result<(i64, i64), ObserverError> {
    loop {
        match query(remaining(deadline)?) {
            Ok(watermarks) => return Ok(watermarks),
            Err(KafkaError::MetadataFetch(
                RDKafkaErrorCode::NotLeaderForPartition | RDKafkaErrorCode::LeaderNotAvailable,
            )) => std::thread::sleep(RETRY_DELAY.min(remaining(deadline)?)),
            Err(error) => return Err(ObserverError::Kafka(error)),
        }
    }
}
