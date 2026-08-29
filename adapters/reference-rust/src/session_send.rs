//! Reference producer translation records model-broker admission and terminal truth.

use std::io::Write;

use testlab_schema::{
    AdapterEvent, AdapterEventEnvelope, BatchRecord, CommandId, OperationId, ProducerId, RecordSpec,
};

use crate::AdapterError;
use crate::broker_client;
use crate::session::emit;
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

pub(crate) fn dispatch_batch<W: Write>(
    state: &AdapterState,
    writer: &mut W,
    command_id: CommandId,
    producer_id: &ProducerId,
    operations: Vec<BatchRecord>,
) -> Result<(), AdapterError> {
    state.require_producer(producer_id)?;
    if operations.is_empty() {
        return Err(AdapterError::Batch("batch was empty".to_owned()));
    }
    let mut terminals = Vec::with_capacity(operations.len());
    for operation in operations {
        emit(
            writer,
            &AdapterEventEnvelope::new(
                command_id.clone(),
                AdapterEvent::OperationAccepted {
                    operation_id: operation.operation_id.clone(),
                },
            ),
        )?;
        let terminal = broker_client::send(
            state.broker_endpoint()?,
            operation.operation_id.clone(),
            operation.record,
        );
        terminals.push((operation.operation_id, terminal));
    }
    for (operation_id, terminal) in terminals {
        emit(
            writer,
            &AdapterEventEnvelope::new(
                command_id.clone(),
                AdapterEvent::OperationTerminal {
                    operation_id,
                    status: terminal.status,
                    code: terminal.code,
                    offset: terminal.offset,
                },
            ),
        )?;
    }
    emit(
        writer,
        &AdapterEventEnvelope::new(
            command_id,
            AdapterEvent::BatchCompleted {
                producer_id: producer_id.clone(),
            },
        ),
    )
}
