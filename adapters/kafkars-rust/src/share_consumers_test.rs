//! Share-consumer configuration tests pin portable limits to public Kafkars policy.

use std::time::Duration;

use testlab_schema::{ShareConsumerBuilderSelection, ShareConsumerFetchConfiguration};

use crate::kafkars_api::Client;
use crate::share_consumers::public_fetch_configuration;

#[test]
fn portable_share_fetch_policy_maps_exact_public_limits() {
    let configuration = ShareConsumerFetchConfiguration {
        max_wait_ms: 250,
        min_bytes: 2,
        max_bytes: 524_288,
        max_records: 3,
        batch_size: 1,
        attempt_timeout_ms: 17_000,
    };
    let fetch = public_fetch_configuration(Some(configuration))
        .unwrap_or_else(|error| panic!("map share fetch policy: {error}"));

    assert_eq!(fetch.max_wait(), Duration::from_millis(250));
    assert_eq!(fetch.min_bytes(), 2);
    assert_eq!(fetch.max_bytes(), 524_288);
    assert_eq!(fetch.max_records(), 3);
    assert_eq!(fetch.batch_size(), 1);
    assert_eq!(fetch.attempt_timeout(), Duration::from_secs(17));

    let builder = client()
        .share_consumer("workers")
        .subscribe(["orders"])
        .rack("rack-a")
        .fetch_config(fetch)
        .membership_start_timeout(Duration::new(23, 456))
        .close_timeout(Duration::from_secs(19));
    assert_eq!(
        crate::share_consumer_configuration::selected(&builder, true)
            .unwrap_or_else(|error| panic!("read share policy: {error}")),
        ShareConsumerBuilderSelection {
            rack: Some("rack-a".to_owned()),
            fetch: Some(configuration),
            membership_start_timeout_ns: 23_000_000_456,
            close_timeout_ms: 19_000,
        }
    );
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

    let builder = client().share_consumer("workers").fetch_config(fetch);
    let selected = crate::share_consumer_configuration::selected(&builder, false)
        .unwrap_or_else(|error| panic!("read omitted share policy: {error}"));
    assert_eq!(selected.rack, None);
    assert_eq!(selected.fetch, None);
    assert_eq!(selected.membership_start_timeout_ns, 30_000_000_000);
    assert_eq!(selected.close_timeout_ms, 30_000);
}

fn client() -> Client {
    Client::builder()
        .bootstrap_servers(["127.0.0.1:1"])
        .build()
        .unwrap_or_else(|error| panic!("start lazy public client: {error}"))
}
