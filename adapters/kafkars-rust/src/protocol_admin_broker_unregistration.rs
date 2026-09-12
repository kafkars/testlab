//! Broker unregistration crosses one explicit destructive public submission boundary.

use std::io::Write;
use std::time::Duration;

use testlab_schema::{
    AdapterEvent, AdapterEventEnvelope, AdminBrokerUnregistration, CommandId,
    UnregisterBrokerCommand,
};

use crate::AdapterError;
use crate::protocol::emit;
use crate::state::AdapterState;

pub(crate) fn unregister<W: Write>(
    state: &AdapterState,
    writer: &mut W,
    command_id: CommandId,
    command: UnregisterBrokerCommand,
) -> Result<(), AdapterError> {
    let result = state
        .client(&command.client_id)?
        .admin()
        .unregister_broker(command.broker_id)
        .deadline_after(Duration::from_millis(command.timeout_ms))
        .submit()
        .wait()
        .map_err(AdapterError::Client)?;
    let throttle_time_ms = u64::try_from(result.throttle_time().as_millis()).map_err(|_| {
        AdapterError::AdminResult(format!(
            "admin operation {} returned an unrepresentable throttle time",
            command.operation_id
        ))
    })?;
    emit(
        writer,
        &AdapterEventEnvelope::new(
            command_id,
            AdapterEvent::BrokerUnregistered(AdminBrokerUnregistration {
                operation_id: command.operation_id,
                broker_id: command.broker_id,
                throttle_time_ms,
            }),
        ),
    )
}
