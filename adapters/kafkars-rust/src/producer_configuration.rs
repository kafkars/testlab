//! Producer configuration maps portable protocol policy to the curated public facade.

use std::time::Duration;

use testlab_schema::{ProducerCompression, ProducerConfiguration, ProducerConfigurationMethod};

use crate::kafkars_api::{
    ClientBuilder, Compression, ProducerConfig, ProducerLimits, ProducerRetryConfig,
};
use crate::state::StateError;

pub(crate) fn apply(
    builder: ClientBuilder,
    method: ProducerConfigurationMethod,
    configuration: ProducerConfiguration,
) -> Result<ClientBuilder, StateError> {
    let limits = configuration.limits;
    let public_limits = ProducerLimits::new(
        portable(limits.retained_bytes, "retained_bytes")?,
        usize::try_from(limits.in_flight_records).map_err(|_| invalid("in_flight_records"))?,
        usize::try_from(limits.waiting_records).map_err(|_| invalid("waiting_records"))?,
        portable(limits.waiting_bytes, "waiting_bytes")?,
        usize::try_from(limits.batch_records).map_err(|_| invalid("batch_records"))?,
        portable(limits.batch_bytes, "batch_bytes")?,
        Duration::from_millis(limits.linger_ms),
    )
    .with_request_bytes(portable(limits.request_bytes, "request_bytes")?)
    .with_max_in_flight_requests_per_broker(usize::from(limits.max_in_flight_requests_per_broker));
    let delivery_timeout = Duration::from_millis(configuration.delivery_timeout_ms);
    let compression = compression(configuration.compression);
    let retry_backoff = Duration::from_millis(configuration.retry_backoff_ms);
    Ok(match method {
        ProducerConfigurationMethod::ProducerConfig => {
            builder.producer_config(ProducerConfig::new(
                delivery_timeout,
                compression,
                ProducerRetryConfig::new(configuration.max_retries, retry_backoff),
                public_limits,
            ))
        }
        ProducerConfigurationMethod::IndividualSetters => builder
            .producer_delivery_timeout(delivery_timeout)
            .producer_compression(compression)
            .producer_retry(configuration.max_retries, retry_backoff)
            .producer_limits(public_limits),
    })
}

fn compression(value: ProducerCompression) -> Compression {
    match value {
        ProducerCompression::None => Compression::None,
        ProducerCompression::Gzip => Compression::Gzip,
        ProducerCompression::Snappy => Compression::Snappy,
        ProducerCompression::Lz4 => Compression::Lz4,
        ProducerCompression::Zstd => Compression::Zstd,
    }
}

fn portable(value: u64, field: &str) -> Result<usize, StateError> {
    usize::try_from(value).map_err(|_| invalid(field))
}

fn invalid(field: &str) -> StateError {
    StateError::ProducerConfiguration(format!("{field} exceeds this adapter target"))
}
