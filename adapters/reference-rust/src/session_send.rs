//! Reference producer translation records model-broker admission and terminal truth.

use std::io::Write;

use testlab_schema::{
    AdapterEvent, AdapterEventEnvelope, CommandId, OperationId, ProducerId, RecordSpec,
};

use crate::broker_client;
use crate::session::{AdapterError, emit};
use crate::state::AdapterState;

pub(crate) fn dispatch_send<W: Write>(
    state: &AdapterState,
    writer: &mut W,
    command_id: CommandId,
    producer_id: &ProducerId,
    operation_id: OperationId,
    record: RecordSpec,
) -> Result<(), AdapterError> {
    state.require_producer(producer_id)?;
    emit(
        writer,
        &AdapterEventEnvelope::new(
            command_id.clone(),
            AdapterEvent::OperationAccepted {
                operation_id: operation_id.clone(),
            },
        ),
    )?;
    let terminal = broker_client::send(state.broker_endpoint()?, operation_id.clone(), record);
    emit(
        writer,
        &AdapterEventEnvelope::new(
            command_id,
            AdapterEvent::OperationTerminal {
                operation_id,
                status: terminal.status,
                code: terminal.code,
                offset: terminal.offset,
            },
        ),
    )
}
