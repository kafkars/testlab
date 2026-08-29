//! Producer command translation preserves public admission and delivery truth.

use std::io::Write;

use testlab_schema::{
    AdapterEvent, AdapterEventEnvelope, BatchRecord, CommandId, OperationId, ProducerId,
    RecordSpec, TerminalStatus,
};

use crate::AdapterError;
use crate::admission_retry::{retry_owned_safe, retry_unadmitted_batch_safe};
use crate::kafkars_api::{KafkaError, Producer, RecordMetadata, TrySendError};
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
    let outcome = execute_send(producer, &operation_id, record)?;
    emit_send_outcome(writer, command_id, operation_id, outcome)
}

#[derive(Debug)]
pub(crate) enum SendOutcome {
    Rejected {
        code: String,
    },
    Accepted {
        status: TerminalStatus,
        code: Option<String>,
        offset: Option<i64>,
    },
}

pub(crate) fn execute_send(
    producer: &Producer,
    operation_id: &OperationId,
    record: RecordSpec,
) -> Result<SendOutcome, AdapterError> {
    let record = normalize::record(record)?;
    let delivery = match retry_owned_safe(record, |record| {
        producer.try_send(record).map_err(TrySendError::into_parts)
    }) {
        Ok(delivery) => delivery,
        Err((_, error)) => {
            eprintln!("Kafkars rejected operation {operation_id}: {error}");
            return Ok(SendOutcome::Rejected {
                code: normalize::error_code(&error),
            });
        }
    };
    let (status, code, offset) = match delivery.wait() {
        Ok(metadata) => (TerminalStatus::Acknowledged, None, Some(metadata.offset())),
        Err(error) => {
            eprintln!("Kafkars delivery failed for {operation_id}: {error}");
            let failure = normalize::delivery_failure(&error);
            (failure.status, Some(failure.code), None)
        }
    };
    Ok(SendOutcome::Accepted {
        status,
        code,
        offset,
    })
}

pub(crate) fn emit_send_outcome<W: Write>(
    writer: &mut W,
    command_id: CommandId,
    operation_id: OperationId,
    outcome: SendOutcome,
) -> Result<(), AdapterError> {
    match outcome {
        SendOutcome::Rejected { code } => emit(
            writer,
            &AdapterEventEnvelope::new(
                command_id,
                AdapterEvent::OperationRejected { operation_id, code },
            ),
        ),
        SendOutcome::Accepted {
            status,
            code,
            offset,
        } => {
            emit(
                writer,
                &AdapterEventEnvelope::new(
                    command_id.clone(),
                    AdapterEvent::OperationAccepted {
                        operation_id: operation_id.clone(),
                    },
                ),
            )?;
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
    }
}

pub(crate) fn dispatch_batch<W: Write>(
    state: &AdapterState,
    writer: &mut W,
    command_id: CommandId,
    producer_id: &ProducerId,
    operations: Vec<BatchRecord>,
) -> Result<(), AdapterError> {
    if operations.is_empty() {
        return Err(AdapterError::BatchResult("batch was empty".to_owned()));
    }
    let operation_ids = operations
        .iter()
        .map(|operation| operation.operation_id.clone())
        .collect::<Vec<_>>();
    let records = operations
        .into_iter()
        .map(|operation| normalize::record(operation.record))
        .collect::<Result<Vec<_>, _>>()?;
    let producer = state.producer(producer_id)?;
    let (deliveries, rejection) = retry_unadmitted_batch_safe(records, |records| {
        let (deliveries, rejection) = producer.send_batch(records).wait().into_parts();
        (deliveries, rejection.map(TrySendError::into_parts))
    });
    let accepted = deliveries.len();
    let (rejected, rejection_code) = match rejection {
        Some((records, error)) => {
            eprintln!("Kafkars rejected batch suffix: {error}");
            (records.len(), Some(normalize::error_code(&error)))
        }
        None => (0, None),
    };
    if accepted.checked_add(rejected) != Some(operation_ids.len()) {
        return Err(AdapterError::BatchResult(format!(
            "{} inputs produced {accepted} deliveries and {rejected} rejections",
            operation_ids.len()
        )));
    }
    emit_batch_admissions(
        writer,
        &command_id,
        &operation_ids,
        accepted,
        rejection_code,
    )?;
    emit_batch_terminals(writer, &command_id, &operation_ids, deliveries)?;
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

fn emit_batch_admissions<W: Write>(
    writer: &mut W,
    command_id: &CommandId,
    operation_ids: &[OperationId],
    accepted: usize,
    rejection_code: Option<String>,
) -> Result<(), AdapterError> {
    for operation_id in &operation_ids[..accepted] {
        emit(
            writer,
            &AdapterEventEnvelope::new(
                command_id.clone(),
                AdapterEvent::OperationAccepted {
                    operation_id: operation_id.clone(),
                },
            ),
        )?;
    }
    if let Some(code) = rejection_code {
        for operation_id in &operation_ids[accepted..] {
            emit(
                writer,
                &AdapterEventEnvelope::new(
                    command_id.clone(),
                    AdapterEvent::OperationRejected {
                        operation_id: operation_id.clone(),
                        code: code.clone(),
                    },
                ),
            )?;
        }
    }
    Ok(())
}

fn emit_batch_terminals<W: Write>(
    writer: &mut W,
    command_id: &CommandId,
    operation_ids: &[OperationId],
    deliveries: Vec<Result<RecordMetadata, KafkaError>>,
) -> Result<(), AdapterError> {
    for (operation_id, delivery) in operation_ids.iter().zip(deliveries) {
        let (status, code, offset) = match delivery {
            Ok(metadata) => (TerminalStatus::Acknowledged, None, Some(metadata.offset())),
            Err(error) => {
                eprintln!("Kafkars batch delivery failed for {operation_id}: {error}");
                let failure = normalize::delivery_failure(&error);
                (failure.status, Some(failure.code), None)
            }
        };
        emit(
            writer,
            &AdapterEventEnvelope::new(
                command_id.clone(),
                AdapterEvent::OperationTerminal {
                    operation_id: operation_id.clone(),
                    status,
                    code,
                    offset,
                },
            ),
        )?;
    }
    Ok(())
}
