//! Assigned-consumer commands retain exact public record bytes and lifecycle truth.

use std::future::Future;
use std::io::Write;
use std::pin::pin;
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use crate::kafkars_api::{AssignedConsumer, ConsumerRecord, RecordBatch, RetryAdvice};
use testlab_schema::{
    AdapterCommand, AdapterEvent, AdapterEventEnvelope, AssignedConsumerFetchEvidence,
    AssignedConsumerReceiveMethod, ByteString, CommandId, ConsumedRecord, ConsumerId, HeaderSpec,
    OperationId,
};

use crate::AdapterError;
use crate::protocol::emit;
use crate::state::AdapterState;

const POLL_SLICE: Duration = Duration::from_millis(10);

#[derive(Debug)]
pub(crate) struct AssignedReceiveOutput {
    records: Vec<ConsumedRecord>,
    fetch_evidence: Option<AssignedConsumerFetchEvidence>,
}

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
            ownership,
        } => create(state, writer, command_id, client_id, consumer_id, ownership),
        AdapterCommand::AssignBeginning {
            consumer_id,
            topic,
            partition,
        } => assign(state, writer, command_id, consumer_id, &topic, partition),
        AdapterCommand::AssignBeginningBatch(command) => {
            state.assign_beginning_batch(
                &command.consumer_id,
                &command.partitions,
                Duration::from_millis(command.timeout_ms),
            )?;
            emit(
                writer,
                &AdapterEventEnvelope::new(
                    command_id,
                    AdapterEvent::AssignmentCompleted {
                        consumer_id: command.consumer_id,
                    },
                ),
            )
        }
        AdapterCommand::ControlAssignedConsumer(command) => {
            let completion = testlab_schema::AssignedConsumerControlCompletion {
                operation_id: command.operation_id.clone(),
                consumer_id: command.consumer_id.clone(),
                control: command.control.kind(),
            };
            state.control_assigned_consumer(&command)?;
            emit(
                writer,
                &AdapterEventEnvelope::new(
                    command_id,
                    AdapterEvent::AssignedConsumerControlCompleted(completion),
                ),
            )
        }
        AdapterCommand::ObserveAssignedConsumerEvent(command) => {
            crate::assigned_consumer_event_observe::observe(state, writer, command_id, command)
        }
        AdapterCommand::Receive {
            consumer_id,
            method,
            receive_id,
            timeout_ms,
        } => receive(
            state,
            writer,
            command_id,
            &consumer_id,
            method,
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
    ownership: testlab_schema::ChildHandleOwnership,
) -> Result<(), AdapterError> {
    state.create_assigned_consumer(client_id, consumer_id.clone(), ownership)?;
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
    method: AssignedConsumerReceiveMethod,
    receive_id: OperationId,
    timeout_ms: u64,
) -> Result<(), AdapterError> {
    let output = receive_records(state.consumer_mut(consumer_id)?, method, timeout_ms)?;
    emit_receive(writer, command_id, receive_id, output)
}

pub(crate) fn receive_records(
    consumer: &mut AssignedConsumer,
    method: AssignedConsumerReceiveMethod,
    timeout_ms: u64,
) -> Result<AssignedReceiveOutput, AdapterError> {
    let timeout = Duration::from_millis(timeout_ms);
    let deadline = Instant::now()
        .checked_add(timeout)
        .ok_or_else(|| AdapterError::ConsumerRecord("receive deadline overflow".to_owned()))?;
    match method {
        AssignedConsumerReceiveMethod::Recv => receive_waiting(consumer, deadline),
        AssignedConsumerReceiveMethod::TryTakeBatch => receive_immediate(consumer, deadline),
    }
}

fn receive_waiting(
    consumer: &mut AssignedConsumer,
    deadline: Instant,
) -> Result<AssignedReceiveOutput, AdapterError> {
    match receive_waiting_batch(consumer, deadline)? {
        Some(batch) => normalize_batch(&batch),
        None => Ok(empty_output()),
    }
}

pub(crate) fn receive_waiting_batch(
    consumer: &mut AssignedConsumer,
    deadline: Instant,
) -> Result<Option<RecordBatch>, AdapterError> {
    let mut receive = pin!(consumer.recv());
    let mut context = Context::from_waker(Waker::noop());
    loop {
        match receive.as_mut().poll(&mut context) {
            Poll::Ready(result) => return result.map_err(AdapterError::Client),
            Poll::Pending => {}
        }
        if Instant::now() >= deadline {
            return Ok(None);
        }
        std::thread::sleep(POLL_SLICE);
    }
}

fn receive_immediate(
    consumer: &mut AssignedConsumer,
    deadline: Instant,
) -> Result<AssignedReceiveOutput, AdapterError> {
    loop {
        match consumer.try_take_batch() {
            Ok(Some(batch)) => return normalize_batch(&batch),
            Ok(None) => {}
            Err(error) if error.retry_advice() == RetryAdvice::RetrySafe => {}
            Err(error) => return Err(AdapterError::Client(error)),
        }
        if Instant::now() >= deadline {
            return Ok(empty_output());
        }
        std::thread::sleep(POLL_SLICE);
    }
}

fn normalize_batch(batch: &RecordBatch) -> Result<AssignedReceiveOutput, AdapterError> {
    let records = batch
        .records()
        .map(|record| normalize_record(&record))
        .collect::<Result<Vec<_>, _>>()?;
    let evidence = batch.evidence();
    Ok(AssignedReceiveOutput {
        records,
        fetch_evidence: Some(AssignedConsumerFetchEvidence {
            topic: evidence.topic().to_owned(),
            topic_uuid: evidence.topic_uuid().into_bytes(),
            partition: evidence.partition(),
            requested_offset: evidence.requested_offset(),
            next_offset: evidence.next_offset(),
            checkpoint_next_offset: batch.checkpoint_next_offset(),
            log_start_offset: evidence.log_start_offset(),
            last_stable_offset: evidence.last_stable_offset(),
            high_watermark: evidence.high_watermark(),
            retained_bytes: evidence.retained_bytes(),
        }),
    })
}

fn empty_output() -> AssignedReceiveOutput {
    AssignedReceiveOutput {
        records: Vec::new(),
        fetch_evidence: None,
    }
}

pub(crate) fn emit_receive<W: Write>(
    writer: &mut W,
    command_id: CommandId,
    receive_id: OperationId,
    output: AssignedReceiveOutput,
) -> Result<(), AdapterError> {
    emit(
        writer,
        &AdapterEventEnvelope::new(
            command_id,
            AdapterEvent::ReceiveCompleted {
                receive_id,
                records: output.records,
                fetch_evidence: output.fetch_evidence,
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
