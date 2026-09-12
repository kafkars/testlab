//! Producer cancellation reports stage-aware outcomes before authoritative terminal truth.

use std::io::Write;
use std::time::{Duration, Instant};

use testlab_schema::{
    AdapterEvent, AdapterEventEnvelope, CancelProducerSendCommand, CommandId,
    ProducerCancellationCompletion, ProducerCancellationOutcome, ProducerSendMethod,
    TerminalStatus,
};

use crate::admission_retry::{retry_owned_until, retry_until};
use crate::kafkars_api::{
    CancellationOutcome, Delivery, ErrorKind, KafkaError, RetryAdvice, Send, TrySendError,
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
    let mut delivery = match command.method {
        ProducerSendMethod::TrySend => {
            match retry_owned_until(
                deadline,
                record,
                |record| producer.try_send(record).map_err(TrySendError::into_parts),
                |error| error.retry_advice() == RetryAdvice::RetrySafe,
            ) {
                Ok(delivery) => CancellableSend::Delivery(delivery),
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
            }
        }
        ProducerSendMethod::Send => CancellableSend::Waiting(producer.send(record)),
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

enum CancellableSend {
    Delivery(Delivery),
    Waiting(Send),
}

impl CancellableSend {
    fn cancel(&mut self) -> Result<CancellationOutcome, KafkaError> {
        match self {
            Self::Delivery(delivery) => delivery.cancel(),
            Self::Waiting(send) => send.cancel(),
        }
    }

    fn wait(self) -> Result<crate::kafkars_api::RecordMetadata, KafkaError> {
        match self {
            Self::Delivery(delivery) => delivery.wait(),
            Self::Waiting(send) => send.wait(),
        }
    }
}

fn cancel(
    delivery: &mut CancellableSend,
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
    delivery: CancellableSend,
) -> Result<(), AdapterError> {
    let (status, code, partition, offset, timestamp_millis) = match delivery.wait() {
        Ok(metadata) => (
            TerminalStatus::Acknowledged,
            None,
            Some(metadata.partition()),
            Some(metadata.offset()),
            metadata.timestamp_milliseconds(),
        ),
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
                partition,
                offset,
                timestamp_millis,
            },
        ),
    )
}

fn terminal_failure(
    error: &KafkaError,
) -> (
    TerminalStatus,
    Option<String>,
    Option<i32>,
    Option<i64>,
    Option<i64>,
) {
    let failure = normalize::delivery_failure(error);
    (failure.status, Some(failure.code), None, None, None)
}
