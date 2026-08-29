//! Share-consumer configuration tests pin portable limits to public Kafkars policy.

use testlab_schema::ShareConsumerFetchConfiguration;

use crate::share_consumers::public_fetch_configuration;

#[test]
fn portable_share_fetch_policy_maps_exact_public_limits() {
    let fetch = public_fetch_configuration(Some(ShareConsumerFetchConfiguration {
        max_records: 3,
        batch_size: 1,
    }))
    .unwrap_or_else(|error| panic!("map share fetch policy: {error}"));

    assert_eq!(fetch.max_records(), 3);
    assert_eq!(fetch.batch_size(), 1);
}

#[test]
fn omitted_share_fetch_policy_retains_testlab_bounds() {
    let fetch = public_fetch_configuration(None)
        .unwrap_or_else(|error| panic!("map default share fetch policy: {error}"));

    assert_eq!(fetch.max_records(), 31);
    assert_eq!(fetch.batch_size(), 31);
}
