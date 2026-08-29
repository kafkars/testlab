//! Producer configuration tests pin every curated public builder selection.

use std::time::Duration;

use testlab_schema::{ProducerCompression, ProducerConfiguration, ProducerLimitsConfiguration};

use crate::kafkars_api::{Client, Compression};

#[test]
fn portable_configuration_selects_every_public_policy_value() {
    for (portable, public) in [
        (ProducerCompression::None, Compression::None),
        (ProducerCompression::Gzip, Compression::Gzip),
        (ProducerCompression::Snappy, Compression::Snappy),
        (ProducerCompression::Lz4, Compression::Lz4),
        (ProducerCompression::Zstd, Compression::Zstd),
    ] {
        let builder = crate::producer_configuration::apply(Client::builder(), config(portable))
            .unwrap_or_else(|error| panic!("apply public producer configuration: {error}"));
        let selected = builder.selected_producer_config();
        assert_eq!(selected.delivery_timeout(), Duration::from_secs(20));
        assert_eq!(selected.compression(), public);
        assert_eq!(selected.retry().max_retries(), 3);
        assert_eq!(selected.retry().backoff(), Duration::from_millis(10));
        let limits = selected.limits();
        assert_eq!(limits.retained_bytes(), 8_388_608);
        assert_eq!(limits.in_flight_records(), 256);
        assert_eq!(limits.waiting_records(), 128);
        assert_eq!(limits.waiting_bytes(), 4_194_304);
        assert_eq!(limits.batch_records(), 32);
        assert_eq!(limits.batch_bytes(), 524_288);
        assert_eq!(limits.request_bytes(), 1_048_576);
        assert_eq!(limits.max_in_flight_requests_per_broker(), 4);
        assert_eq!(limits.linger(), Duration::from_millis(3));
    }
}

fn config(compression: ProducerCompression) -> ProducerConfiguration {
    ProducerConfiguration {
        delivery_timeout_ms: 20_000,
        compression,
        max_retries: 3,
        retry_backoff_ms: 10,
        limits: ProducerLimitsConfiguration {
            retained_bytes: 8_388_608,
            in_flight_records: 256,
            waiting_records: 128,
            waiting_bytes: 4_194_304,
            batch_records: 32,
            batch_bytes: 524_288,
            request_bytes: 1_048_576,
            max_in_flight_requests_per_broker: 4,
            linger_ms: 3,
        },
    }
}
