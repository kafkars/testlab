//! Producer configuration maps portable protocol policy to the curated public facade.

use std::time::Duration;

use testlab_schema::{
    ProducerCompression, ProducerConfiguration, ProducerConfigurationMethod,
    ProducerLimitsConfiguration,
};

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

pub(crate) fn selected(builder: &ClientBuilder) -> Result<ProducerConfiguration, StateError> {
    let complete = builder.selected_producer_config();
    let delivery_timeout = builder.selected_producer_delivery_timeout();
    let compression = builder.selected_producer_compression();
    let retry = builder.selected_producer_retry();
    let limits = builder.selected_producer_limits();
    if complete.delivery_timeout() != delivery_timeout
        || complete.compression() != compression
        || complete.retry() != retry
        || complete.limits() != limits
    {
        return Err(StateError::ProducerConfiguration(
            "public client-builder producer policy getters disagreed".to_owned(),
        ));
    }
    Ok(ProducerConfiguration {
        delivery_timeout_ms: whole_millis(delivery_timeout, "delivery_timeout")?,
        compression: portable_compression(compression),
        max_retries: retry.max_retries(),
        retry_backoff_ms: whole_millis(retry.backoff(), "retry_backoff")?,
        limits: ProducerLimitsConfiguration {
            retained_bytes: portable_u64(limits.retained_bytes(), "retained_bytes")?,
            in_flight_records: portable_u32(limits.in_flight_records(), "in_flight_records")?,
            waiting_records: portable_u32(limits.waiting_records(), "waiting_records")?,
            waiting_bytes: portable_u64(limits.waiting_bytes(), "waiting_bytes")?,
            batch_records: portable_u32(limits.batch_records(), "batch_records")?,
            batch_bytes: portable_u64(limits.batch_bytes(), "batch_bytes")?,
            request_bytes: portable_u64(limits.request_bytes(), "request_bytes")?,
            max_in_flight_requests_per_broker: portable_u8(
                limits.max_in_flight_requests_per_broker(),
                "max_in_flight_requests_per_broker",
            )?,
            linger_ms: whole_millis(limits.linger(), "linger")?,
        },
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

fn portable_compression(value: Compression) -> ProducerCompression {
    match value {
        Compression::None => ProducerCompression::None,
        Compression::Gzip => ProducerCompression::Gzip,
        Compression::Snappy => ProducerCompression::Snappy,
        Compression::Lz4 => ProducerCompression::Lz4,
        Compression::Zstd => ProducerCompression::Zstd,
    }
}

fn portable(value: u64, field: &str) -> Result<usize, StateError> {
    usize::try_from(value).map_err(|_| invalid(field))
}

fn invalid(field: &str) -> StateError {
    StateError::ProducerConfiguration(format!("{field} exceeds this adapter target"))
}

fn whole_millis(value: Duration, field: &str) -> Result<u64, StateError> {
    let millis = u64::try_from(value.as_millis()).map_err(|_| selected_invalid(field))?;
    if Duration::from_millis(millis) != value {
        return Err(selected_invalid(field));
    }
    Ok(millis)
}

fn portable_u64(value: usize, field: &str) -> Result<u64, StateError> {
    u64::try_from(value).map_err(|_| selected_invalid(field))
}

fn portable_u32(value: usize, field: &str) -> Result<u32, StateError> {
    u32::try_from(value).map_err(|_| selected_invalid(field))
}

fn portable_u8(value: usize, field: &str) -> Result<u8, StateError> {
    u8::try_from(value).map_err(|_| selected_invalid(field))
}

fn selected_invalid(field: &str) -> StateError {
    StateError::ProducerConfiguration(format!(
        "selected {field} was not representable in the portable protocol"
    ))
}
