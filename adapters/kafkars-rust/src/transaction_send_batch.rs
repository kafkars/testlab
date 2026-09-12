//! Transactional batch staging preserves one public homogeneous-batch observation.

use std::io::Write;
use std::thread;
use std::time::{Duration, Instant};

use testlab_schema::{
    AdapterEvent, AdapterEventEnvelope, BatchRecord, CommandId, OperationId, TerminalStatus,
};

use crate::AdapterError;
use crate::kafkars_api::{RetryAdvice, Transaction};
use crate::normalize;
use crate::protocol::emit;

pub(crate) fn send<W: Write>(
    transaction: &mut Transaction<'_>,
    writer: &mut W,
    command_id: &CommandId,
    operations: Vec<BatchRecord>,
    deadline: Instant,
) -> Result<(), AdapterError> {
    let (topic, partition) = homogeneous_target(&operations)?;
    let operation_ids = operations
        .iter()
        .map(|operation| operation.operation_id.clone())
        .collect::<Vec<_>>();
    let mut records = operations
        .into_iter()
        .map(|operation| normalize::record(operation.record))
        .collect::<Result<Vec<_>, _>>()?;
    let observer = loop {
        match transaction.send_batch(records, crate::transaction_execute::remaining(deadline)?) {
            Ok(observer) => break observer,
            Err(rejection) => {
                let (returned, error) = rejection.into_parts();
                if error.retry_advice() == RetryAdvice::RetrySafe && Instant::now() < deadline {
                    records = returned;
                    thread::sleep(Duration::from_millis(1));
                    continue;
                }
                emit_rejections(writer, command_id, &operation_ids, &error)?;
                return Err(AdapterError::Client(error));
            }
        }
    };
    for operation_id in &operation_ids {
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
    match observer.wait() {
        Ok(metadata) => emit_successes(
            writer,
            command_id,
            &operation_ids,
            &topic,
            partition,
            metadata,
        ),
        Err(error) => {
            let failure = normalize::delivery_failure(&error);
            for operation_id in operation_ids {
                emit(
                    writer,
                    &AdapterEventEnvelope::new(
                        command_id.clone(),
                        AdapterEvent::OperationTerminal {
                            operation_id,
                            status: failure.status,
                            code: Some(failure.code.clone()),
                            partition: None,
                            offset: None,
                            timestamp_millis: None,
                        },
                    ),
                )?;
            }
            Err(AdapterError::Client(error))
        }
    }
}

fn homogeneous_target(operations: &[BatchRecord]) -> Result<(String, i32), AdapterError> {
    let first = operations
        .first()
        .ok_or_else(|| invalid("batch is empty"))?;
    let topic = first.record.topic.clone();
    let partition = first.record.partition;
    if operations
        .iter()
        .any(|operation| operation.record.topic != topic || operation.record.partition != partition)
    {
        return Err(invalid("records do not share one topic and partition"));
    }
    Ok((topic, partition))
}

fn emit_rejections<W: Write>(
    writer: &mut W,
    command_id: &CommandId,
    operation_ids: &[OperationId],
    error: &crate::kafkars_api::KafkaError,
) -> Result<(), AdapterError> {
    let code = normalize::error_code(error);
    for operation_id in operation_ids {
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
    Ok(())
}

fn emit_successes<W: Write>(
    writer: &mut W,
    command_id: &CommandId,
    operation_ids: &[OperationId],
    topic: &str,
    partition: i32,
    metadata: crate::kafkars_api::TransactionBatchMetadata,
) -> Result<(), AdapterError> {
    let count = operation_ids.len();
    let offset_span = i64::try_from(count.saturating_sub(1))
        .map_err(|_| invalid("record count exceeds offset range"))?;
    let expected_last = metadata
        .base_offset()
        .checked_add(offset_span)
        .ok_or_else(|| invalid("batch offset range overflow"))?;
    if metadata.topic() != topic
        || metadata.partition() != partition
        || metadata.record_count() != count
        || metadata.last_offset() != expected_last
    {
        return Err(invalid(
            "batch acknowledgment did not match the requested record set",
        ));
    }
    for (index, operation_id) in operation_ids.iter().enumerate() {
        let offset = metadata
            .base_offset()
            .checked_add(i64::try_from(index).map_err(|_| invalid("record index overflow"))?)
            .ok_or_else(|| invalid("record offset overflow"))?;
        emit(
            writer,
            &AdapterEventEnvelope::new(
                command_id.clone(),
                AdapterEvent::OperationTerminal {
                    operation_id: operation_id.clone(),
                    status: TerminalStatus::TransactionStaged,
                    code: None,
                    partition: Some(partition),
                    offset: Some(offset),
                    timestamp_millis: metadata.timestamp(),
                },
            ),
        )?;
    }
    Ok(())
}

fn invalid(detail: &str) -> AdapterError {
    AdapterError::TransactionResult(format!("transactional batch {detail}"))
}
