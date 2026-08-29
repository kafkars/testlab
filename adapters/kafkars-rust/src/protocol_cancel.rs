//! Producer cancellation reports stage-aware outcomes before authoritative terminal truth.

use std::io::Write;
use std::time::{Duration, Instant};

use testlab_schema::{
    AdapterEvent, AdapterEventEnvelope, CancelProducerSendCommand, CommandId,
    ProducerCancellationCompletion, ProducerCancellationOutcome, TerminalStatus,
};

use crate::admission_retry::{retry_owned_until, retry_until};
use crate::kafkars_api::{
    CancellationOutcome, Delivery, ErrorKind, KafkaError, RetryAdvice, TrySendError,
};
use crate::protocol::emit;
use crate::state::AdapterState;
use crate::{AdapterError, normalize};

pub(crate) fn dispatch<W: Write>(
    state: &AdapterState,
    writer: &mut W,
    command_id: CommandId,
    command: CancelProducerSendCommand,
) -> Result<(), AdapterError> {
    let started = Instant::now();
    let deadline = started
        .checked_add(Duration::from_millis(command.timeout_ms))
        .unwrap_or(started);
    let producer = state.producer(&command.producer_id)?;
    let record = normalize::record(command.record)?;
    let mut delivery = match retry_owned_until(
        deadline,
        record,
        |record| producer.try_send(record).map_err(TrySendError::into_parts),
        |error| error.retry_advice() == RetryAdvice::RetrySafe,
    ) {
        Ok(delivery) => delivery,
        Err((_, error)) => {
            emit(
                writer,
                &AdapterEventEnvelope::new(
                    command_id,
                    AdapterEvent::OperationRejected {
                        operation_id: command.operation_id,
                        code: normalize::error_code(&error),
                    },
                ),
            )?;
            return Err(AdapterError::Client(error));
        }
    };
    emit(
        writer,
        &AdapterEventEnvelope::new(
            command_id.clone(),
            AdapterEvent::OperationAccepted {
                operation_id: command.operation_id.clone(),
            },
        ),
    )?;
    let outcomes = vec![
        cancel(&mut delivery, deadline)?,
        cancel(&mut delivery, deadline)?,
    ];
    emit_terminal(writer, &command_id, command.operation_id.clone(), delivery)?;
    emit(
        writer,
        &AdapterEventEnvelope::new(
            command_id,
            AdapterEvent::ProducerCancellationCompleted(ProducerCancellationCompletion {
                operation_id: command.operation_id,
                outcomes,
            }),
        ),
    )
}

fn cancel(
    delivery: &mut Delivery,
    deadline: Instant,
) -> Result<ProducerCancellationOutcome, AdapterError> {
    retry_until(
        deadline,
        || delivery.cancel(),
        |error| error.kind() == ErrorKind::Backpressure,
    )
    .map(map_outcome)
    .map_err(AdapterError::Client)
}

pub(crate) fn map_outcome(outcome: CancellationOutcome) -> ProducerCancellationOutcome {
    match outcome {
        CancellationOutcome::CancelledNotSent => ProducerCancellationOutcome::CancelledNotSent,
        CancellationOutcome::TooLate => ProducerCancellationOutcome::TooLate,
        CancellationOutcome::AlreadyTerminal => ProducerCancellationOutcome::AlreadyTerminal,
    }
}

fn emit_terminal<W: Write>(
    writer: &mut W,
    command_id: &CommandId,
    operation_id: testlab_schema::OperationId,
    delivery: Delivery,
) -> Result<(), AdapterError> {
    let (status, code, offset) = match delivery.wait() {
        Ok(metadata) => (TerminalStatus::Acknowledged, None, Some(metadata.offset())),
        Err(error) => terminal_failure(&error),
    };
    emit(
        writer,
        &AdapterEventEnvelope::new(
            command_id.clone(),
            AdapterEvent::OperationTerminal {
                operation_id,
                status,
                code,
                offset,
            },
        ),
    )
}

fn terminal_failure(error: &KafkaError) -> (TerminalStatus, Option<String>, Option<i64>) {
    let failure = normalize::delivery_failure(error);
    (failure.status, Some(failure.code), None)
}
