//! Assigned-consumer policy maps only to Kafkars' public client builder.

use testlab_schema::{AssignedConsumerConfiguration, AssignedConsumerReadIsolation};

use crate::kafkars_api::{ClientBuilder, ReadIsolation};
use crate::state::StateError;

pub(crate) fn apply(
    builder: ClientBuilder,
    configuration: AssignedConsumerConfiguration,
) -> Result<ClientBuilder, StateError> {
    let mut builder = builder
        .assigned_consumer_read_isolation(public_read_isolation(configuration.read_isolation));
    if let Some(fetch) = configuration.fetch {
        builder =
            builder.assigned_consumer_fetch(crate::consumer_configuration::public_fetch(fetch)?);
    }
    if let Some(limits) = configuration.limits {
        builder =
            builder.assigned_consumer_limits(crate::consumer_configuration::public_limits(limits)?);
    }
    Ok(builder)
}

pub(crate) const fn public_read_isolation(
    isolation: AssignedConsumerReadIsolation,
) -> ReadIsolation {
    match isolation {
        AssignedConsumerReadIsolation::ReadUncommitted => ReadIsolation::ReadUncommitted,
        AssignedConsumerReadIsolation::ReadCommitted => ReadIsolation::ReadCommitted,
    }
}
