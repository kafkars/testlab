//! Group-consumer configuration tests pin portable policy to public Kafkars enums.

use std::time::Duration;

use testlab_schema::{
    GroupClassicAssignor, GroupConsumerConfiguration, GroupOffsetReset, GroupReadIsolation,
};

use crate::group_consumer_configuration::{
    apply_fetch_and_limits, apply_runtime_configuration, public_classic_group_config,
};
use crate::group_consumers::{public_classic_assignor, public_offset_reset, public_read_isolation};
use crate::kafkars_api::{ClassicGroupAssignor, Client, OffsetReset, ReadIsolation};

#[test]
fn public_group_shutdown_surface_is_facade_only() {
    let surface: fn(&mut crate::kafkars_api::Consumer) = request_and_observe_shutdown;
    assert_eq!(
        std::mem::size_of_val(&surface),
        std::mem::size_of::<fn(&mut crate::kafkars_api::Consumer)>()
    );
}

#[test]
fn portable_group_policy_maps_every_public_selection() {
    assert_eq!(
        public_classic_assignor(GroupClassicAssignor::Range),
        ClassicGroupAssignor::Range
    );
    assert_eq!(
        public_classic_assignor(GroupClassicAssignor::CooperativeSticky),
        ClassicGroupAssignor::CooperativeSticky
    );
    assert_eq!(
        public_offset_reset(GroupOffsetReset::Error),
        OffsetReset::Error
    );
    assert_eq!(
        public_offset_reset(GroupOffsetReset::Earliest),
        OffsetReset::Earliest
    );
    assert_eq!(
        public_offset_reset(GroupOffsetReset::Latest),
        OffsetReset::Latest
    );
    assert_eq!(
        public_read_isolation(GroupReadIsolation::ReadUncommitted),
        ReadIsolation::ReadUncommitted
    );
    assert_eq!(
        public_read_isolation(GroupReadIsolation::ReadCommitted),
        ReadIsolation::ReadCommitted
    );
}

#[test]
fn portable_classic_timing_maps_every_public_selection() {
    let configuration = GroupConsumerConfiguration {
        offset_reset: GroupOffsetReset::Earliest,
        read_isolation: GroupReadIsolation::ReadUncommitted,
        fetch: None,
        limits: None,
        processing_timeout_ms: None,
        membership_start_timeout_ms: None,
        seek_timeout_ms: None,
        close_timeout_ms: None,
        group_instance_id: None,
        classic_assignor: None,
        classic_session_timeout_ms: Some(11_000),
        classic_rebalance_timeout_ms: Some(31_000),
        classic_heartbeat_interval_ms: Some(4_000),
        classic_heartbeat_attempt_timeout_ms: Some(12_000),
        classic_rejoin_backoff_ms: Some(2_000),
        classic_rejoin_attempt_timeout_ms: Some(32_000),
    };
    let selected = public_classic_group_config(&configuration)
        .unwrap_or_else(|| panic!("explicit classic timing must map"));
    assert_eq!(selected.session_timeout(), Duration::from_secs(11));
    assert_eq!(selected.rebalance_timeout(), Duration::from_secs(31));
    assert_eq!(selected.heartbeat_interval(), Duration::from_secs(4));
    assert_eq!(
        selected.heartbeat_attempt_timeout(),
        Duration::from_secs(12)
    );
    assert_eq!(selected.rejoin_backoff(), Duration::from_secs(2));
    assert_eq!(selected.rejoin_attempt_timeout(), Duration::from_secs(32));
}

#[test]
fn portable_group_runtime_maps_every_public_deadline() {
    let configuration = GroupConsumerConfiguration {
        offset_reset: GroupOffsetReset::Earliest,
        read_isolation: GroupReadIsolation::ReadUncommitted,
        fetch: Some(testlab_schema::ConsumerFetchConfiguration {
            max_wait_ms: 250,
            min_bytes: 2,
            max_bytes: 524_288,
            partition_max_bytes: 262_144,
            attempt_timeout_ms: 17_000,
        }),
        limits: Some(testlab_schema::ConsumerLimitsConfiguration {
            in_flight_fetches: 3,
            buffered_batches: 4,
            buffered_bytes: 2_097_152,
            max_batch_bytes: 524_288,
        }),
        processing_timeout_ms: Some(61_000),
        membership_start_timeout_ms: Some(23_000),
        seek_timeout_ms: Some(17_000),
        close_timeout_ms: Some(19_000),
        group_instance_id: None,
        classic_assignor: None,
        classic_session_timeout_ms: None,
        classic_rebalance_timeout_ms: None,
        classic_heartbeat_interval_ms: None,
        classic_heartbeat_attempt_timeout_ms: None,
        classic_rejoin_backoff_ms: None,
        classic_rejoin_attempt_timeout_ms: None,
    };
    let client = Client::builder()
        .bootstrap_servers(["127.0.0.1:1"])
        .build()
        .unwrap_or_else(|error| panic!("start lazy public client: {error}"));
    let selected = apply_runtime_configuration(client.consumer("workers"), &configuration);
    let selected = apply_fetch_and_limits(selected, &configuration)
        .unwrap_or_else(|error| panic!("apply group Fetch policy: {error}"));
    assert_eq!(
        selected.selected_processing_timeout(),
        Duration::from_secs(61)
    );
    assert_eq!(
        selected.selected_membership_start_timeout(),
        Duration::from_secs(23)
    );
    assert_eq!(selected.selected_seek_timeout(), Duration::from_secs(17));
    assert_eq!(selected.selected_close_timeout(), Duration::from_secs(19));
    let fetch = selected.selected_fetch_config();
    assert_eq!(fetch.max_wait(), Duration::from_millis(250));
    assert_eq!(fetch.min_bytes(), 2);
    assert_eq!(fetch.max_bytes(), 524_288);
    assert_eq!(fetch.partition_max_bytes(), 262_144);
    assert_eq!(fetch.attempt_timeout(), Duration::from_secs(17));
    let limits = selected.selected_limits();
    assert_eq!(limits.in_flight_fetches(), 3);
    assert_eq!(limits.buffered_batches(), 4);
    assert_eq!(limits.buffered_bytes(), 2_097_152);
    assert_eq!(limits.max_batch_bytes(), 524_288);
}

fn request_and_observe_shutdown(consumer: &mut crate::kafkars_api::Consumer) {
    let control = consumer.control();
    control.request_shutdown();
    let _next = consumer.next_event();
}
