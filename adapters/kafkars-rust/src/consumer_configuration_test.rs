//! Consumer configuration maps every portable value to the public Kafkars policy.

use std::time::Duration;

use testlab_schema::{ConsumerFetchConfiguration, ConsumerLimitsConfiguration};

#[test]
fn portable_fetch_and_limits_map_exactly() {
    let expected_fetch = ConsumerFetchConfiguration {
        max_wait_ms: 250,
        min_bytes: 2,
        max_bytes: 524_288,
        partition_max_bytes: 262_144,
        attempt_timeout_ms: 17_000,
    };
    let fetch = crate::consumer_configuration::public_fetch(expected_fetch)
        .unwrap_or_else(|error| panic!("map Fetch policy: {error}"));
    assert_eq!(fetch.max_wait(), Duration::from_millis(250));
    assert_eq!(fetch.min_bytes(), 2);
    assert_eq!(fetch.max_bytes(), 524_288);
    assert_eq!(fetch.partition_max_bytes(), 262_144);
    assert_eq!(fetch.attempt_timeout(), Duration::from_secs(17));
    assert_eq!(
        crate::consumer_configuration::selected_fetch(fetch)
            .unwrap_or_else(|error| panic!("read Fetch policy: {error}")),
        expected_fetch
    );

    let expected_limits = ConsumerLimitsConfiguration {
        in_flight_fetches: 3,
        buffered_batches: 4,
        buffered_bytes: 2_097_152,
        max_batch_bytes: 524_288,
    };
    let limits = crate::consumer_configuration::public_limits(expected_limits)
        .unwrap_or_else(|error| panic!("map consumer limits: {error}"));
    assert_eq!(limits.in_flight_fetches(), 3);
    assert_eq!(limits.buffered_batches(), 4);
    assert_eq!(limits.buffered_bytes(), 2_097_152);
    assert_eq!(limits.max_batch_bytes(), 524_288);
    assert_eq!(
        crate::consumer_configuration::selected_limits(limits)
            .unwrap_or_else(|error| panic!("read consumer limits: {error}")),
        expected_limits
    );
}
