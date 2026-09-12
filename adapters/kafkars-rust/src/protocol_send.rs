//! Producer command translation preserves public admission and delivery truth.

use std::io::Write;

use testlab_schema::{
    AdapterEvent, AdapterEventEnvelope, BatchRecord, CommandId, OperationId, ProducerId,
    ProducerPartitioning, ProducerSendMethod, RecordSpec, TerminalStatus,
};

use crate::AdapterError;
use crate::admission_retry::{retry_owned_safe, retry_unadmitted_batch_safe};
use crate::kafkars_api::{KafkaError, Producer, RecordMetadata, TopicUuid, TrySendError};
use crate::normalize;
use crate::protocol::emit;
use crate::protocol_send_outcome::metadata_receipt;
pub(crate) use crate::protocol_send_outcome::{SendOutcome, emit_send_outcome};
use crate::state::AdapterState;

pub(crate) fn dispatch_send<W: Write>(
    state: &AdapterState,
    writer: &mut W,
    command_id: CommandId,
    producer_id: &ProducerId,
    operation_id: OperationId,
    method: ProducerSendMethod,
    partitioning: ProducerPartitioning,
    validate_topic_uuid: bool,
    record: RecordSpec,
) -> Result<(), AdapterError> {
    let topic_uuid = validate_topic_uuid
        .then(|| {
            crate::producer_topic_uuid::resolve(state, producer_id, &operation_id, &record.topic)
        })
        .transpose()?;
    let producer = state.producer(producer_id)?;
    let outcome = match method {
        ProducerSendMethod::TrySend => {
            execute_send_with_topic_uuid(producer, &operation_id, partitioning, topic_uuid, record)?
        }
        ProducerSendMethod::Send => {
            execute_waiting_send(producer, partitioning, topic_uuid, record)?
        }
    };
    emit_send_outcome(writer, command_id, operation_id, outcome)
}

pub(crate) fn execute_send(
    producer: &Producer,
    operation_id: &OperationId,
    partitioning: ProducerPartitioning,
    record: RecordSpec,
) -> Result<SendOutcome, AdapterError> {
    execute_send_with_topic_uuid(producer, operation_id, partitioning, None, record)
}

fn execute_send_with_topic_uuid(
    producer: &Producer,
    operation_id: &OperationId,
    partitioning: ProducerPartitioning,
    topic_uuid: Option<TopicUuid>,
    record: RecordSpec,
) -> Result<SendOutcome, AdapterError> {
    let mut record = normalize::producer_record(record, partitioning)?;
    if let Some(topic_uuid) = topic_uuid {
        record = record.expected_topic_uuid(topic_uuid);
    }
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
    let outcome = match delivery.wait() {
        Ok(metadata) => SendOutcome::acknowledged(metadata),
        Err(error) => {
            eprintln!("Kafkars delivery failed for {operation_id}: {error}");
            let failure = normalize::delivery_failure(&error);
            SendOutcome::failed(failure.status, failure.code)
        }
    };
    Ok(outcome)
}

fn execute_waiting_send(
    producer: &Producer,
    partitioning: ProducerPartitioning,
    topic_uuid: Option<TopicUuid>,
    record: RecordSpec,
) -> Result<SendOutcome, AdapterError> {
    let mut record = normalize::producer_record(record, partitioning)?;
    if let Some(topic_uuid) = topic_uuid {
        record = record.expected_topic_uuid(topic_uuid);
    }
    let outcome = match producer.send(record).wait() {
        Ok(metadata) => SendOutcome::acknowledged(metadata),
        Err(error) => {
            let failure = normalize::delivery_failure(&error);
            SendOutcome::failed(failure.status, failure.code)
        }
    };
    Ok(outcome)
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
        let (status, code, partition, offset, timestamp_millis, receipt) = match delivery {
            Ok(metadata) => {
                let receipt = metadata_receipt(&metadata);
                (
                    TerminalStatus::Acknowledged,
                    None,
                    Some(metadata.partition()),
                    Some(metadata.offset()),
                    metadata.timestamp_milliseconds(),
                    Some(receipt),
                )
            }
            Err(error) => {
                eprintln!("Kafkars batch delivery failed for {operation_id}: {error}");
                let failure = normalize::delivery_failure(&error);
                (failure.status, Some(failure.code), None, None, None, None)
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
                    partition,
                    offset,
                    timestamp_millis,
                    receipt,
                },
            ),
        )?;
    }
    Ok(())
}
