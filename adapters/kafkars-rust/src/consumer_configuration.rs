//! Portable consumer policy maps to and from Kafkars' public Fetch and capacity types.

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

pub(crate) fn selected_fetch(
    selected: ConsumerFetchConfig,
) -> Result<ConsumerFetchConfiguration, StateError> {
    Ok(ConsumerFetchConfiguration {
        max_wait_ms: whole_millis(selected.max_wait(), "fetch.max_wait")?,
        min_bytes: selected_u64(selected.min_bytes(), "fetch.min_bytes")?,
        max_bytes: selected_u64(selected.max_bytes(), "fetch.max_bytes")?,
        partition_max_bytes: selected_u64(
            selected.partition_max_bytes(),
            "fetch.partition_max_bytes",
        )?,
        attempt_timeout_ms: whole_millis(selected.attempt_timeout(), "fetch.attempt_timeout")?,
    })
}

pub(crate) fn selected_limits(
    selected: ConsumerLimits,
) -> Result<ConsumerLimitsConfiguration, StateError> {
    Ok(ConsumerLimitsConfiguration {
        in_flight_fetches: selected_u32(selected.in_flight_fetches(), "limits.in_flight_fetches")?,
        buffered_batches: selected_u32(selected.buffered_batches(), "limits.buffered_batches")?,
        buffered_bytes: selected_u64(selected.buffered_bytes(), "limits.buffered_bytes")?,
        max_batch_bytes: selected_u64(selected.max_batch_bytes(), "limits.max_batch_bytes")?,
    })
}

pub(crate) fn whole_millis(value: Duration, field: &str) -> Result<u64, StateError> {
    let millis = u64::try_from(value.as_millis()).map_err(|_| selected_invalid(field))?;
    if Duration::from_millis(millis) != value {
        return Err(selected_invalid(field));
    }
    Ok(millis)
}

fn portable(value: u64, field: &str) -> Result<usize, StateError> {
    usize::try_from(value).map_err(|_| invalid(field))
}

fn invalid(field: &str) -> StateError {
    StateError::ConsumerConfiguration(format!("{field} exceeds this adapter target"))
}

fn selected_u64(value: usize, field: &str) -> Result<u64, StateError> {
    u64::try_from(value).map_err(|_| selected_invalid(field))
}

fn selected_u32(value: usize, field: &str) -> Result<u32, StateError> {
    u32::try_from(value).map_err(|_| selected_invalid(field))
}

fn selected_invalid(field: &str) -> StateError {
    StateError::ConsumerConfiguration(format!(
        "selected {field} was not representable in the portable protocol"
    ))
}
