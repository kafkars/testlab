//! Portable consumer policy maps to Kafkars' public Fetch and capacity types.

use std::time::Duration;

use testlab_schema::{ConsumerFetchConfiguration, ConsumerLimitsConfiguration};

use crate::kafkars_api::{ConsumerFetchConfig, ConsumerLimits};
use crate::state::StateError;

pub(crate) fn public_fetch(
    configuration: ConsumerFetchConfiguration,
) -> Result<ConsumerFetchConfig, StateError> {
    Ok(ConsumerFetchConfig::new(
        Duration::from_millis(configuration.max_wait_ms),
        portable(configuration.min_bytes, "fetch.min_bytes")?,
        portable(configuration.max_bytes, "fetch.max_bytes")?,
        portable(
            configuration.partition_max_bytes,
            "fetch.partition_max_bytes",
        )?,
        Duration::from_millis(configuration.attempt_timeout_ms),
    ))
}

pub(crate) fn public_limits(
    configuration: ConsumerLimitsConfiguration,
) -> Result<ConsumerLimits, StateError> {
    Ok(ConsumerLimits::new(
        usize::try_from(configuration.in_flight_fetches)
            .map_err(|_| invalid("limits.in_flight_fetches"))?,
        usize::try_from(configuration.buffered_batches)
            .map_err(|_| invalid("limits.buffered_batches"))?,
        portable(configuration.buffered_bytes, "limits.buffered_bytes")?,
        portable(configuration.max_batch_bytes, "limits.max_batch_bytes")?,
    ))
}

fn portable(value: u64, field: &str) -> Result<usize, StateError> {
    usize::try_from(value).map_err(|_| invalid(field))
}

fn invalid(field: &str) -> StateError {
    StateError::ConsumerConfiguration(format!("{field} exceeds this adapter target"))
}
