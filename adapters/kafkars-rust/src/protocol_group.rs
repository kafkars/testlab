//! Classic-group commands retain public records and checkpoint truth.

use std::future::Future;
use std::io::Write;
use std::pin::pin;
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use kafkars::{
    ConsumerBatch, GroupConsumerRecord, GroupMembershipEpoch as PublicGroupMembershipEpoch,
    RetryAdvice,
};
use testlab_schema::{
    AdapterCommand, AdapterEvent, AdapterEventEnvelope, ByteString, CommandId, ConsumedRecord,
    ConsumerId, GroupMembershipEpoch, HeaderSpec, OperationId,
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
        AdapterCommand::CreateGroupConsumer {
            client_id,
            consumer_id,
            group_id,
            topic,
            protocol,
        } => {
            state.create_group_consumer(
                client_id,
                consumer_id.clone(),
                group_id,
                topic,
                protocol,
            )?;
            emit(
                writer,
                &AdapterEventEnvelope::new(
                    command_id,
                    AdapterEvent::GroupConsumerCreated { consumer_id },
                ),
            )
        }
        AdapterCommand::GroupReceive {
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
        AdapterCommand::CloseGroupConsumer { consumer_id } => {
            state.close_group_consumer(&consumer_id)?;
            emit(
                writer,
                &AdapterEventEnvelope::new(
                    command_id,
                    AdapterEvent::GroupConsumerClosed { consumer_id },
                ),
            )
        }
        _ => Err(AdapterError::ConsumerRecord(
            "non-group command reached group dispatcher".to_owned(),
        )),
    }
}

fn receive<W: Write>(
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
    let (records, committed) = match receive_batch(state, consumer_id, deadline)? {
        Some(batch) => {
            let records = batch
                .records()
                .map(|record| normalize_record(&record))
                .collect::<Result<Vec<_>, _>>()?;
            let checkpoint = batch.checkpoint();
            let commit_timeout = deadline.saturating_duration_since(Instant::now());
            state
                .group_consumer_mut(consumer_id)?
                .try_commit(checkpoint, commit_timeout)
                .map_err(|error| AdapterError::Client(error.into_parts().1))?
                .wait()
                .map_err(|error| AdapterError::Client(error.into_parts().1))?;
            (records, true)
        }
        None => (Vec::new(), false),
    };
    let group_epoch = public_group_epoch(state, consumer_id, deadline)?;
    emit(
        writer,
        &AdapterEventEnvelope::new(
            command_id,
            AdapterEvent::GroupReceiveCompleted {
                receive_id,
                records,
                committed,
                group_epoch,
            },
        ),
    )
}

fn public_group_epoch(
    state: &mut AdapterState,
    consumer_id: &ConsumerId,
    deadline: Instant,
) -> Result<Option<GroupMembershipEpoch>, AdapterError> {
    loop {
        match state.group_consumer_mut(consumer_id)?.group_metadata() {
            Ok(Some(metadata)) => {
                return Ok(Some(match metadata.membership_epoch() {
                    PublicGroupMembershipEpoch::Classic { generation_id } => {
                        GroupMembershipEpoch::Classic { generation_id }
                    }
                    PublicGroupMembershipEpoch::Consumer { member_epoch } => {
                        GroupMembershipEpoch::Consumer { member_epoch }
                    }
                }));
            }
            Ok(None) => {}
            Err(error) if error.retry_advice() == RetryAdvice::RetrySafe => {}
            Err(error) => return Err(AdapterError::Client(error)),
        }
        if Instant::now() >= deadline {
            return Ok(None);
        }
        std::thread::sleep(POLL_SLICE);
    }
}

fn receive_batch(
    state: &mut AdapterState,
    consumer_id: &ConsumerId,
    deadline: Instant,
) -> Result<Option<ConsumerBatch>, AdapterError> {
    if let Some(error) = state.group_consumer_mut(consumer_id)?.startup_error() {
        return Err(AdapterError::Client(error));
    }
    let result = {
        let mut receive = pin!(state.group_consumer_mut(consumer_id)?.recv());
        let mut context = Context::from_waker(Waker::noop());
        loop {
            if let Poll::Ready(result) = receive.as_mut().poll(&mut context) {
                break Some(result);
            }
            if Instant::now() >= deadline {
                break None;
            }
            std::thread::sleep(POLL_SLICE);
        }
    };
    match result {
        Some(Ok(None)) => {
            if let Some(error) = state.group_consumer_mut(consumer_id)?.startup_error() {
                return Err(AdapterError::Client(error));
            }
            Ok(None)
        }
        Some(result) => result.map_err(AdapterError::Client),
        None => Ok(None),
    }
}

fn normalize_record(record: &GroupConsumerRecord<'_>) -> Result<ConsumedRecord, AdapterError> {
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
