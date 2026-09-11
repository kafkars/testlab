//! Assigned-consumer configuration maps every portable isolation selection.

use testlab_schema::AssignedConsumerReadIsolation;

use crate::assigned_consumer_configuration::public_read_isolation;
use crate::kafkars_api::ReadIsolation;

#[test]
fn portable_read_isolation_maps_every_public_selection() {
    assert_eq!(
        public_read_isolation(AssignedConsumerReadIsolation::ReadUncommitted),
        ReadIsolation::ReadUncommitted
    );
    assert_eq!(
        public_read_isolation(AssignedConsumerReadIsolation::ReadCommitted),
        ReadIsolation::ReadCommitted
    );
}
