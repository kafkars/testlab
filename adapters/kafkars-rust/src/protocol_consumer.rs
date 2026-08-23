//! Assigned-consumer commands retain exact public record bytes and lifecycle truth.

use std::future::Future;
use std::io::Write;
use std::pin::pin;
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use kafkars::ConsumerRecord;
use testlab_schema::{
    AdapterCommand, AdapterEvent, AdapterEventEnvelope, ByteString, CommandId, ConsumedRecord,
    ConsumerId, HeaderSpec, OperationId,
};

use crate::AdapterError;
use crate::protocol::emit;
use crate::state::AdapterState;

const POLL_SLICE: Duration = Duration::from_millis(10);

pub(crate) fn dispatch<W: Write>(
    state: &mut AdapterState,
    writer: &mut W,
    command_id: CommandId,
    command: AdapterCommand,
) -> Result<(), AdapterError> {
    match command {
        AdapterCommand::CreateAssignedConsumer {
            client_id,
            consumer_id,
        } => create(state, writer, command_id, client_id, consumer_id),
        AdapterCommand::AssignBeginning {
            consumer_id,
            topic,
            partition,
        } => assign(state, writer, command_id, consumer_id, &topic, partition),
        AdapterCommand::Receive {
            consumer_id,
            receive_id,
            timeout_ms,
        } => receive(
            state,
            writer,
            command_id,
            &consumer_id,
            receive_id,
            timeout_ms,
        ),
        AdapterCommand::CloseAssignedConsumer { consumer_id } => {
            close(state, writer, command_id, consumer_id)
        }
        _ => Err(AdapterError::ConsumerRecord(
            "non-consumer command reached consumer dispatcher".to_owned(),
        )),
    }
}

pub(crate) fn create<W: Write>(
    state: &mut AdapterState,
    writer: &mut W,
    command_id: CommandId,
    client_id: testlab_schema::ClientId,
    consumer_id: ConsumerId,
) -> Result<(), AdapterError> {
    state.create_assigned_consumer(client_id, consumer_id.clone())?;
    emit(
        writer,
        &AdapterEventEnvelope::new(
            command_id,
            AdapterEvent::AssignedConsumerCreated { consumer_id },
        ),
    )
}

pub(crate) fn assign<W: Write>(
    state: &mut AdapterState,
    writer: &mut W,
    command_id: CommandId,
    consumer_id: ConsumerId,
    topic: &str,
    partition: i32,
) -> Result<(), AdapterError> {
    state.assign_beginning(&consumer_id, topic, partition)?;
    emit(
        writer,
        &AdapterEventEnvelope::new(
            command_id,
            AdapterEvent::AssignmentCompleted { consumer_id },
        ),
    )
}

pub(crate) fn receive<W: Write>(
    state: &mut AdapterState,
    writer: &mut W,
    command_id: CommandId,
    consumer_id: &ConsumerId,
    receive_id: OperationId,
    timeout_ms: u64,
) -> Result<(), AdapterError> {
    let timeout = Duration::from_millis(timeout_ms);
    let deadline = Instant::now()
        .checked_add(timeout)
        .ok_or_else(|| AdapterError::ConsumerRecord("receive deadline overflow".to_owned()))?;
    let mut receive = pin!(state.consumer_mut(consumer_id)?.recv());
    let mut context = Context::from_waker(Waker::noop());
    let records = loop {
        match receive.as_mut().poll(&mut context) {
            Poll::Ready(Ok(Some(batch))) => {
                break batch
                    .records()
                    .map(|record| normalize_record(&record))
                    .collect::<Result<Vec<_>, _>>()?;
            }
            Poll::Ready(Ok(None)) => break Vec::new(),
            Poll::Ready(Err(error)) => return Err(AdapterError::Client(error)),
            Poll::Pending => {}
        }
        if Instant::now() >= deadline {
            break Vec::new();
        }
        std::thread::sleep(POLL_SLICE);
    };
    emit(
        writer,
        &AdapterEventEnvelope::new(
            command_id,
            AdapterEvent::ReceiveCompleted {
                receive_id,
                records,
            },
        ),
    )
}

pub(crate) fn close<W: Write>(
    state: &mut AdapterState,
    writer: &mut W,
    command_id: CommandId,
    consumer_id: ConsumerId,
) -> Result<(), AdapterError> {
    state.close_assigned_consumer(&consumer_id)?;
    emit(
        writer,
        &AdapterEventEnvelope::new(
            command_id,
            AdapterEvent::AssignedConsumerClosed { consumer_id },
        ),
    )
}

fn normalize_record(record: &ConsumerRecord<'_>) -> Result<ConsumedRecord, AdapterError> {
    let headers = record
        .headers()
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
        topic: record.topic().to_owned(),
        partition: record.partition(),
        offset: record.offset(),
        timestamp_millis: record.timestamp_millis(),
        key: record.key().map(ByteString::hex),
        value: record.value().map(ByteString::hex),
        headers,
    })
}
