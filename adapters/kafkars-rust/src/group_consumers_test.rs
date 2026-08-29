//! Group-consumer configuration tests pin portable policy to public Kafkars enums.

use testlab_schema::{GroupOffsetReset, GroupReadIsolation};

use crate::group_consumers::{public_offset_reset, public_read_isolation};
use crate::kafkars_api::{OffsetReset, ReadIsolation};

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

fn request_and_observe_shutdown(consumer: &mut crate::kafkars_api::Consumer) {
    let control = consumer.control();
    control.request_shutdown();
    let _next = consumer.next_event();
}
