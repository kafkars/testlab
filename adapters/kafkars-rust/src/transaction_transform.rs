//! Transactional transforms retain public group fences through atomic offset transfer.

use std::io::Write;
use std::thread;
use std::time::{Duration, Instant};

use crate::kafkars_api::{RetryAdvice, Transaction};
use testlab_schema::{
    AdapterEvent, AdapterEventEnvelope, CommandId, ConsumerId, TransactionalTransformCommand,
    TransactionalTransformCompletion,
};

use crate::AdapterError;
use crate::protocol::emit;
use crate::state::AdapterState;

pub(crate) fn execute<W: Write>(
    state: &mut AdapterState,
    writer: &mut W,
    command_id: CommandId,
    command: TransactionalTransformCommand,
) -> Result<(), AdapterError> {
    let producer_id = command.producer_id.clone();
    let mut owner = state.take_transactional_producer(&producer_id)?;
    let result = execute_owned(state, &mut owner.producer, writer, command_id, command);
    let restored = state.restore_transactional_producer(producer_id, owner);
    match (result, restored) {
        (Err(error), _) => Err(error),
        (Ok(()), Err(error)) => Err(error.into()),
        (Ok(()), Ok(())) => Ok(()),
    }
}

fn execute_owned<W: Write>(
    state: &mut AdapterState,
    producer: &mut crate::kafkars_api::TransactionalProducer,
    writer: &mut W,
    command_id: CommandId,
    command: TransactionalTransformCommand,
) -> Result<(), AdapterError> {
    let deadline = Instant::now()
        .checked_add(Duration::from_millis(command.timeout_ms))
        .ok_or_else(|| AdapterError::ConsumerRecord("transform deadline overflow".to_owned()))?;
    let batch = crate::protocol_group::receive_batch(state, &command.consumer_id, deadline)?
        .ok_or_else(|| {
            AdapterError::ConsumerRecord("transactional transform receive timed out".to_owned())
        })?;
    let records = batch
        .records()
        .map(|record| crate::protocol_group::normalize_record(&record))
        .collect::<Result<Vec<_>, _>>()?;
    let metadata = group_metadata(state, &command.consumer_id, deadline)?;
    let group_id = metadata.group_id().to_owned();
    let group_epoch = crate::protocol_group::normalize_group_epoch(metadata.membership_epoch());
    let topic = batch.topic().to_owned();
    let partition = batch.partition();
    let next_offset = batch.checkpoint_next_offset();
    let checkpoint = batch.checkpoint();
    begin_and_execute(
        producer,
        writer,
        command_id,
        command,
        metadata,
        checkpoint,
        records,
        group_id,
        group_epoch,
        topic,
        partition,
        next_offset,
        deadline,
    )
}

#[allow(
    clippy::too_many_arguments,
    reason = "retained public transform evidence"
)]
fn begin_and_execute<W: Write>(
    producer: &mut crate::kafkars_api::TransactionalProducer,
    writer: &mut W,
    command_id: CommandId,
    command: TransactionalTransformCommand,
    metadata: crate::kafkars_api::GroupMetadata,
    checkpoint: crate::kafkars_api::Checkpoint,
    records: Vec<testlab_schema::ConsumedRecord>,
    group_id: String,
    group_epoch: testlab_schema::GroupMembershipEpoch,
    topic: String,
    partition: i32,
    next_offset: i64,
    deadline: Instant,
) -> Result<(), AdapterError> {
    loop {
        match producer.begin() {
            Ok(transaction) => {
                return execute_started(
                    transaction,
                    writer,
                    command_id,
                    command,
                    metadata,
                    checkpoint,
                    records,
                    group_id,
                    group_epoch,
                    topic,
                    partition,
                    next_offset,
                    deadline,
                );
            }
            Err(error)
                if error.retry_advice() == RetryAdvice::RetrySafe && Instant::now() < deadline =>
            {
                thread::sleep(Duration::from_millis(1));
            }
            Err(error) => return Err(AdapterError::Client(error)),
        }
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "retained public transform evidence"
)]
fn execute_started<W: Write>(
    mut transaction: Transaction<'_>,
    writer: &mut W,
    command_id: CommandId,
    command: TransactionalTransformCommand,
    metadata: crate::kafkars_api::GroupMetadata,
    checkpoint: crate::kafkars_api::Checkpoint,
    records: Vec<testlab_schema::ConsumedRecord>,
    group_id: String,
    group_epoch: testlab_schema::GroupMembershipEpoch,
    topic: String,
    partition: i32,
    next_offset: i64,
    deadline: Instant,
) -> Result<(), AdapterError> {
    for operation in command.operations {
        crate::transaction_execute::send(
            &mut transaction,
            writer,
            &command_id,
            operation,
            deadline,
        )?;
    }
    send_offsets(&mut transaction, metadata, checkpoint, deadline)?;
    crate::transaction_execute::end(transaction, command.disposition, deadline)?;
    emit(
        writer,
        &AdapterEventEnvelope::new(
            command_id,
            AdapterEvent::TransactionalTransformCompleted(TransactionalTransformCompletion {
                transaction_id: command.transaction_id,
                disposition: command.disposition,
                consumer_id: command.consumer_id,
                records,
                group_id,
                topic,
                partition,
                next_offset,
                group_epoch,
            }),
        ),
    )
}

fn group_metadata(
    state: &mut AdapterState,
    consumer_id: &ConsumerId,
    deadline: Instant,
) -> Result<crate::kafkars_api::GroupMetadata, AdapterError> {
    crate::protocol_group::public_group_metadata(state, consumer_id, deadline)?.ok_or_else(|| {
        AdapterError::ConsumerRecord(
            "transactional transform has no current public group metadata".to_owned(),
        )
    })
}

fn send_offsets(
    transaction: &mut Transaction<'_>,
    mut metadata: crate::kafkars_api::GroupMetadata,
    mut checkpoint: crate::kafkars_api::Checkpoint,
    deadline: Instant,
) -> Result<(), AdapterError> {
    loop {
        match transaction.send_offsets(
            metadata,
            checkpoint,
            crate::transaction_execute::remaining(deadline)?,
        ) {
            Ok(observer) => return observer.wait().map_err(AdapterError::Client),
            Err(rejection) => {
                let (returned_metadata, returned_checkpoint, error) = rejection.into_parts();
                metadata = returned_metadata;
                checkpoint = returned_checkpoint;
                if error.retry_advice() != RetryAdvice::RetrySafe || Instant::now() >= deadline {
                    return Err(AdapterError::Client(error));
                }
                thread::sleep(Duration::from_millis(1));
            }
        }
    }
}
