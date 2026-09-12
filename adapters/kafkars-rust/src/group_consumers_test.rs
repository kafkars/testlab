//! Group-consumer configuration tests pin portable policy to public Kafkars enums.

use std::time::Duration;

use testlab_schema::{
    GroupClassicAssignor, GroupConsumerConfiguration, GroupOffsetReset, GroupReadIsolation,
};

use crate::group_consumer_configuration::public_classic_group_config;
use crate::group_consumers::{public_classic_assignor, public_offset_reset, public_read_isolation};
use crate::kafkars_api::{ClassicGroupAssignor, OffsetReset, ReadIsolation};

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

fn request_and_observe_shutdown(consumer: &mut crate::kafkars_api::Consumer) {
    let control = consumer.control();
    control.request_shutdown();
    let _next = consumer.next_event();
}
