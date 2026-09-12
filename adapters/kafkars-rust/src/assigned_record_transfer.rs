//! Owned direct-consumer records retain source evidence through producer settlement.

use std::io::Write;
use std::sync::Arc;
use std::time::{Duration, Instant};

use testlab_schema::{
    AdapterEvent, AdapterEventEnvelope, AssignedRecordConversionMethod,
    AssignedRecordTransferCommand, AssignedRecordTransferCompletion, ByteString, CommandId,
    ConsumedRecord, HeaderSpec, ProducerReceipt, RECORD_TRANSFER_OPERATION_HEADER, TerminalStatus,
};

use crate::AdapterError;
use crate::kafkars_api::{OwnedConsumerHeader, OwnedConsumerRecord, RetainedSourceRecord};
use crate::protocol::emit;
use crate::state::AdapterState;

pub(crate) fn execute<W: Write>(
    state: &mut AdapterState,
    writer: &mut W,
    command_id: CommandId,
    command: AssignedRecordTransferCommand,
) -> Result<(), AdapterError> {
    let deadline = Instant::now()
        .checked_add(Duration::from_millis(command.timeout_ms))
        .ok_or_else(|| AdapterError::ConsumerRecord("transfer deadline overflow".to_owned()))?;
    let batch = crate::protocol_consumer::receive_waiting_batch(
        state.consumer_mut(&command.consumer_id)?,
        deadline,
    )?
    .ok_or_else(|| AdapterError::ConsumerRecord("record transfer timed out".to_owned()))?;
    if batch.len() != 1 {
        return Err(AdapterError::ConsumerRecord(format!(
            "record transfer expected one source record, observed {}",
            batch.len()
        )));
    }
    let mut records = match command.method {
        AssignedRecordConversionMethod::IntoOwnedBatch => batch.into_owned().into_records(),
        AssignedRecordConversionMethod::IntoOwnedRecords => batch.into_owned_records(),
    };
    let source = records.next().ok_or_else(|| {
        AdapterError::ConsumerRecord("record transfer source disappeared".to_owned())
    })?;
    drop(records);
    let source_record = normalize_owned(&source)?;
    let (record, retained_source) = transfer(source, &command)?;
    let result = state.producer(&command.producer_id)?.send(record).wait();
    let (status, code, partition, offset, timestamp_millis, receipt) = terminal(result);
    emit_terminal(
        writer,
        &command_id,
        &command.operation_id,
        status,
        code.clone(),
        partition,
        offset,
        timestamp_millis,
        receipt.clone(),
    )?;
    let retained_source_record = normalize_retained(&retained_source)?;
    emit(
        writer,
        &AdapterEventEnvelope::new(
            command_id,
            AdapterEvent::AssignedRecordTransferCompleted(AssignedRecordTransferCompletion {
                operation_id: command.operation_id,
                source_record,
                retained_source_record,
                status,
                code,
                partition,
                offset,
                timestamp_millis,
                receipt,
            }),
        ),
    )
}

fn transfer(
    source: OwnedConsumerRecord,
    command: &AssignedRecordTransferCommand,
) -> Result<(crate::kafkars_api::Record, RetainedSourceRecord), AdapterError> {
    let target_topic = Arc::<str>::from(command.target_topic.as_str());
    let (record, retained) = match source.try_into_record(target_topic) {
        Ok(transferred) => transferred,
        Err(rejection) => {
            let (_source, _target) = rejection.into_parts();
            return Err(AdapterError::ConsumerRecord(
                "owned record transfer allocation failed".to_owned(),
            ));
        }
    };
    Ok((
        record.partition(command.target_partition).header(
            RECORD_TRANSFER_OPERATION_HEADER,
            command.operation_id.as_str().as_bytes().to_vec(),
        ),
        retained,
    ))
}

type Terminal = (
    TerminalStatus,
    Option<String>,
    Option<i32>,
    Option<i64>,
    Option<i64>,
    Option<ProducerReceipt>,
);

fn terminal(
    result: Result<crate::kafkars_api::RecordMetadata, crate::kafkars_api::KafkaError>,
) -> Terminal {
    match result {
        Ok(metadata) => {
            let receipt = crate::protocol_send_outcome::metadata_receipt(&metadata);
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
            let failure = crate::normalize::delivery_failure(&error);
            (failure.status, Some(failure.code), None, None, None, None)
        }
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "complete public transfer terminal"
)]
fn emit_terminal<W: Write>(
    writer: &mut W,
    command_id: &CommandId,
    operation_id: &testlab_schema::OperationId,
    status: TerminalStatus,
    code: Option<String>,
    partition: Option<i32>,
    offset: Option<i64>,
    timestamp_millis: Option<i64>,
    receipt: Option<ProducerReceipt>,
) -> Result<(), AdapterError> {
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
    )
}

fn normalize_owned(record: &OwnedConsumerRecord) -> Result<ConsumedRecord, AdapterError> {
    normalize_record(
        record.topic(),
        record.partition(),
        record.offset(),
        record.timestamp_millis(),
        record.key(),
        record.value(),
        record.headers(),
    )
}

fn normalize_retained(record: &RetainedSourceRecord) -> Result<ConsumedRecord, AdapterError> {
    normalize_record(
        record.topic(),
        record.partition(),
        record.offset(),
        record.timestamp_millis(),
        record.key(),
        record.value(),
        record.headers(),
    )
}

#[allow(clippy::too_many_arguments, reason = "exact public source record")]
fn normalize_record<'record>(
    topic: &str,
    partition: i32,
    offset: i64,
    timestamp_millis: Option<i64>,
    key: Option<&[u8]>,
    value: Option<&[u8]>,
    headers: impl Iterator<Item = OwnedConsumerHeader<'record>>,
) -> Result<ConsumedRecord, AdapterError> {
    let headers = headers
        .map(|header| {
            let name = String::from_utf8(header.key().to_vec())
                .map_err(|error| AdapterError::ConsumerRecord(error.to_string()))?;
            Ok(HeaderSpec {
                name,
                value: header.value().map(ByteString::hex),
            })
        })
        .collect::<Result<Vec<_>, AdapterError>>()?;
    Ok(ConsumedRecord {
        topic: topic.to_owned(),
        partition,
        offset,
        timestamp_millis,
        key: key.map(ByteString::hex),
        value: value.map(ByteString::hex),
        headers,
    })
}
