//! Producer command translation preserves public admission and delivery truth.

use std::io::Write;

use testlab_schema::{
    AdapterEvent, AdapterEventEnvelope, CommandId, OperationId, ProducerId, RecordSpec,
    TerminalStatus,
};

use crate::AdapterError;
use crate::normalize;
use crate::protocol::emit;
use crate::state::AdapterState;

pub(crate) fn dispatch_send<W: Write>(
    state: &AdapterState,
    writer: &mut W,
    command_id: CommandId,
    producer_id: &ProducerId,
    operation_id: OperationId,
    record: RecordSpec,
) -> Result<(), AdapterError> {
    let producer = state.producer(producer_id)?;
    let record = normalize::record(record)?;
    let delivery = match producer.try_send(record) {
        Ok(delivery) => delivery,
        Err(rejection) => {
            let (_, error) = rejection.into_parts();
            eprintln!("Kafkars rejected operation {operation_id}: {error}");
            return emit(
                writer,
                &AdapterEventEnvelope::new(
                    command_id,
                    AdapterEvent::OperationRejected {
                        operation_id,
                        code: normalize::error_code(&error),
                    },
                ),
            );
        }
    };
    emit(
        writer,
        &AdapterEventEnvelope::new(
            command_id.clone(),
            AdapterEvent::OperationAccepted {
                operation_id: operation_id.clone(),
            },
        ),
    )?;
    let (status, code, offset) = match delivery.wait() {
        Ok(metadata) => (TerminalStatus::Acknowledged, None, Some(metadata.offset())),
        Err(error) => {
            eprintln!("Kafkars delivery failed for {operation_id}: {error}");
            let failure = normalize::delivery_failure(&error);
            (failure.status, Some(failure.code), None)
        }
    };
    emit(
        writer,
        &AdapterEventEnvelope::new(
            command_id,
            AdapterEvent::OperationTerminal {
                operation_id,
                status,
                code,
                offset,
            },
        ),
    )
}
