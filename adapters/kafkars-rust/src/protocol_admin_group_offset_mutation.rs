//! Consumer-group offset mutations validate exact packaged topic-partition results.

use std::io::Write;
use std::time::{Duration, Instant};

use crate::kafkars_api::{ConsumerGroupOffsetAlteration, KafkaError, RetryAdvice, TopicPartition};
use testlab_schema::{
    AdapterEvent, AdapterEventEnvelope, AdminConsumerGroupOffsetCompletion,
    AlterConsumerGroupOffsetCommand, CommandId, DeleteConsumerGroupOffsetCommand,
};

use crate::AdapterError;
use crate::admission_retry::retry_until_with_remaining;
use crate::protocol::emit;
use crate::protocol_admin_result::take_single_result;
use crate::state::AdapterState;

pub(crate) fn alter<W: Write>(
    state: &AdapterState,
    writer: &mut W,
    command_id: CommandId,
    command: AlterConsumerGroupOffsetCommand,
) -> Result<(), AdapterError> {
    let deadline = deadline_after(command.timeout_ms);
    let client = state.client(&command.client_id)?;
    let result = retry_until_with_remaining(
        deadline,
        |remaining| {
            let alteration = ConsumerGroupOffsetAlteration::new(
                command.topic.clone(),
                command.partition,
                command.offset,
            );
            client
                .admin()
                .alter_consumer_group_offsets(command.group_id.clone(), [alteration])
                .deadline_after(remaining)
                .submit()
                .wait()
        },
        retry_safe,
    )
    .map_err(AdapterError::Client)?;
    validate_partition_result(
        result.into_offsets().into_entries(),
        &command.operation_id,
        &command.topic,
        command.partition,
        "consumer-group offset alteration",
    )?;
    emit_completion(
        writer,
        command_id,
        AdapterEvent::ConsumerGroupOffsetAltered(completion(
            command.operation_id,
            command.group_id,
            command.topic,
            command.partition,
        )),
    )
}

pub(crate) fn delete<W: Write>(
    state: &AdapterState,
    writer: &mut W,
    command_id: CommandId,
    command: DeleteConsumerGroupOffsetCommand,
) -> Result<(), AdapterError> {
    let deadline = deadline_after(command.timeout_ms);
    let client = state.client(&command.client_id)?;
    let result = retry_until_with_remaining(
        deadline,
        |remaining| {
            client
                .admin()
                .delete_consumer_group_offsets(
                    command.group_id.clone(),
                    [TopicPartition::new(
                        command.topic.clone(),
                        command.partition,
                    )],
                )
                .deadline_after(remaining)
                .submit()
                .wait()
        },
        retry_safe,
    )
    .map_err(AdapterError::Client)?;
    validate_partition_result(
        result.into_offsets().into_entries(),
        &command.operation_id,
        &command.topic,
        command.partition,
        "consumer-group offset deletion",
    )?;
    emit_completion(
        writer,
        command_id,
        AdapterEvent::ConsumerGroupOffsetDeleted(completion(
            command.operation_id,
            command.group_id,
            command.topic,
            command.partition,
        )),
    )
}

fn validate_partition_result<V>(
    entries: Vec<(TopicPartition, Result<V, KafkaError>)>,
    operation_id: &testlab_schema::OperationId,
    expected_topic: &str,
    expected_partition: i32,
    resource: &str,
) -> Result<V, AdapterError> {
    take_single_result(
        entries,
        operation_id,
        |topic_partition| {
            topic_partition.topic() == expected_topic
                && topic_partition.partition() == expected_partition
                && topic_partition.start_position().is_none()
        },
        resource,
    )
}

fn completion(
    operation_id: testlab_schema::OperationId,
    group_id: String,
    topic: String,
    partition: i32,
) -> AdminConsumerGroupOffsetCompletion {
    AdminConsumerGroupOffsetCompletion {
        operation_id,
        group_id,
        topic,
        partition,
    }
}

fn deadline_after(timeout_ms: u64) -> Instant {
    let started = Instant::now();
    started
        .checked_add(Duration::from_millis(timeout_ms))
        .unwrap_or(started)
}

fn retry_safe(error: &KafkaError) -> bool {
    error.retry_advice() == RetryAdvice::RetrySafe
}

fn emit_completion<W: Write>(
    writer: &mut W,
    command_id: CommandId,
    event: AdapterEvent,
) -> Result<(), AdapterError> {
    emit(writer, &AdapterEventEnvelope::new(command_id, event))
}
