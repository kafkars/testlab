//! Share-consumer configuration tests pin portable limits to public Kafkars policy.

use std::time::Duration;

use testlab_schema::ShareConsumerFetchConfiguration;

use crate::share_consumers::public_fetch_configuration;

#[test]
fn portable_share_fetch_policy_maps_exact_public_limits() {
    let fetch = public_fetch_configuration(Some(ShareConsumerFetchConfiguration {
        max_wait_ms: 250,
        min_bytes: 2,
        max_bytes: 524_288,
        max_records: 3,
        batch_size: 1,
        attempt_timeout_ms: 17_000,
    }))
    .unwrap_or_else(|error| panic!("map share fetch policy: {error}"));

    assert_eq!(fetch.max_wait(), Duration::from_millis(250));
    assert_eq!(fetch.min_bytes(), 2);
    assert_eq!(fetch.max_bytes(), 524_288);
    assert_eq!(fetch.max_records(), 3);
    assert_eq!(fetch.batch_size(), 1);
    assert_eq!(fetch.attempt_timeout(), Duration::from_secs(17));
}

#[test]
fn omitted_share_fetch_policy_retains_testlab_bounds() {
    let fetch = public_fetch_configuration(None)
        .unwrap_or_else(|error| panic!("map default share fetch policy: {error}"));

    assert_eq!(fetch.max_wait(), Duration::from_millis(500));
    assert_eq!(fetch.min_bytes(), 1);
    assert_eq!(fetch.max_bytes(), 1_048_576);
    assert_eq!(fetch.max_records(), 31);
    assert_eq!(fetch.batch_size(), 31);
    assert_eq!(fetch.attempt_timeout(), Duration::from_secs(30));
}
