//! Assigned-consumer policy maps only to Kafkars' public client builder.

use testlab_schema::{AssignedConsumerConfiguration, AssignedConsumerReadIsolation};

use crate::kafkars_api::{ClientBuilder, ReadIsolation};

pub(crate) const fn apply(
    builder: ClientBuilder,
    configuration: AssignedConsumerConfiguration,
) -> ClientBuilder {
    builder.assigned_consumer_read_isolation(public_read_isolation(configuration.read_isolation))
}

pub(crate) const fn public_read_isolation(
    isolation: AssignedConsumerReadIsolation,
) -> ReadIsolation {
    match isolation {
        AssignedConsumerReadIsolation::ReadUncommitted => ReadIsolation::ReadUncommitted,
        AssignedConsumerReadIsolation::ReadCommitted => ReadIsolation::ReadCommitted,
    }
}
