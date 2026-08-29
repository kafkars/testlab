//! Topic-admin mutations require one exact packaged public result before success.

use std::io::Write;
use std::time::{Duration, Instant};

use crate::kafkars_api::{DeleteRecordsTarget, KafkaError, NewPartitions, NewTopic, RetryAdvice};
use testlab_schema::{
    AdapterCommand, AdapterEvent, AdapterEventEnvelope, AdminRecordsDeleted, AdminTopicCompletion,
    CommandId, CreatePartitionsCommand, CreateTopicCommand, DeleteRecordsCommand,
    DeleteTopicCommand,
};

use crate::AdapterError;
use crate::admission_retry::retry_until_with_remaining;
use crate::protocol::emit;
use crate::protocol_admin_create_topics_batch;
use crate::protocol_admin_result::{deleted_records_low_watermark, validate_single_topic_result};
use crate::protocol_admin_validation_event::{partition_increase, topic_creation};
use crate::state::AdapterState;

pub(crate) fn dispatch<W: Write>(
    state: &AdapterState,
    writer: &mut W,
    command_id: CommandId,
    command: AdapterCommand,
) -> Result<(), AdapterError> {
    match command {
        AdapterCommand::CreateTopic(command) => create_topic(state, writer, command_id, command),
        AdapterCommand::CreateTopicsBatch(command) => {
            protocol_admin_create_topics_batch::create(state, writer, command_id, command)
        }
        AdapterCommand::CreatePartitions(command) => {
            create_partitions(state, writer, command_id, command)
        }
        AdapterCommand::DeleteTopic(command) => delete_topic(state, writer, command_id, command),
        AdapterCommand::DeleteRecords(command) => {
            delete_records(state, writer, command_id, command)
        }
        _ => Err(AdapterError::AdminResult(
            "non-admin-write command reached admin write dispatcher".to_owned(),
        )),
    }
}

fn create_topic<W: Write>(
    state: &AdapterState,
    writer: &mut W,
    command_id: CommandId,
    command: CreateTopicCommand,
) -> Result<(), AdapterError> {
    let deadline = deadline_after(command.timeout_ms);
    let validate_only = command.validate_only;
    let client = state.client(&command.client_id)?;
    let result = retry_until_with_remaining(
        deadline,
        |remaining| {
            let request = NewTopic::new(command.topic.clone(), command.partitions)
                .replication_factor(command.replication_factor);
            client
                .admin()
                .create_topics([request])
                .validate_only(validate_only)
                .deadline_after(remaining)
                .submit()
                .wait()
        },
        retry_safe,
    )
    .map_err(AdapterError::Client)?;
    validate_single_topic_result(result.into_entries(), &command.operation_id, &command.topic)?;
    emit_completion(
        writer,
        command_id,
        topic_creation(
            validate_only,
            AdminTopicCompletion {
                operation_id: command.operation_id,
                topic: command.topic,
            },
        ),
    )
}

fn create_partitions<W: Write>(
    state: &AdapterState,
    writer: &mut W,
    command_id: CommandId,
    command: CreatePartitionsCommand,
) -> Result<(), AdapterError> {
    let deadline = deadline_after(command.timeout_ms);
    let validate_only = command.validate_only;
    let client = state.client(&command.client_id)?;
    let result = retry_until_with_remaining(
        deadline,
        |remaining| {
            let request = NewPartitions::new(command.topic.clone(), command.total_count);
            client
                .admin()
                .create_partitions([request])
                .validate_only(validate_only)
                .deadline_after(remaining)
                .submit()
                .wait()
        },
        retry_safe,
    )
    .map_err(AdapterError::Client)?;
    validate_single_topic_result(result.into_entries(), &command.operation_id, &command.topic)?;
    emit_completion(
        writer,
        command_id,
        partition_increase(
            validate_only,
            AdminTopicCompletion {
                operation_id: command.operation_id,
                topic: command.topic,
            },
        ),
    )
}

fn delete_topic<W: Write>(
    state: &AdapterState,
    writer: &mut W,
    command_id: CommandId,
    command: DeleteTopicCommand,
) -> Result<(), AdapterError> {
    let deadline = deadline_after(command.timeout_ms);
    let client = state.client(&command.client_id)?;
    let result = retry_until_with_remaining(
        deadline,
        |remaining| {
            client
                .admin()
                .delete_topics([command.topic.clone()])
                .deadline_after(remaining)
                .submit()
                .wait()
        },
        retry_safe,
    )
    .map_err(AdapterError::Client)?;
    validate_single_topic_result(result.into_entries(), &command.operation_id, &command.topic)?;
    emit_completion(
        writer,
        command_id,
        AdapterEvent::TopicDeleted(AdminTopicCompletion {
            operation_id: command.operation_id,
            topic: command.topic,
        }),
    )
}

fn delete_records<W: Write>(
    state: &AdapterState,
    writer: &mut W,
    command_id: CommandId,
    command: DeleteRecordsCommand,
) -> Result<(), AdapterError> {
    let client = state.client(&command.client_id)?;
    let result = client
        .admin()
        .delete_records([DeleteRecordsTarget::before_offset(
            command.topic.clone(),
            command.partition,
            command.before_offset,
        )])
        .deadline_after(Duration::from_millis(command.timeout_ms))
        .submit()
        .wait()
        .map_err(AdapterError::Client)?;
    let entries = result
        .into_records()
        .into_entries()
        .into_iter()
        .map(|(topic_partition, result)| {
            (topic_partition, result.map(|record| record.low_watermark()))
        })
        .collect();
    let low_watermark = deleted_records_low_watermark(
        entries,
        &command.operation_id,
        &command.topic,
        command.partition,
    )?;
    emit_completion(
        writer,
        command_id,
        AdapterEvent::RecordsDeleted(AdminRecordsDeleted {
            operation_id: command.operation_id,
            topic: command.topic,
            partition: command.partition,
            low_watermark,
        }),
    )
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
