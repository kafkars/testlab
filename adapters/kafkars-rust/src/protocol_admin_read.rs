//! Read-only admin commands preserve exact packaged public result identities.

use std::io::Write;
use std::time::{Duration, Instant};

use crate::kafkars_api::{
    KafkaError, ListOffsetsQuery, OffsetSpec, ReadIsolation, RetryAdvice, TopicPartition,
};
use testlab_schema::{
    AdapterCommand, AdapterEvent, AdapterEventEnvelope, AdminConsumerGroupOffsetListing,
    AdminOffsetListing, AdminOffsetPosition, AdminTopicDescription, AdminTopicsListing, CommandId,
    DescribeTopicCommand, ListConsumerGroupOffsetsCommand, ListOffsetsCommand, ListTopicsCommand,
};

use crate::AdapterError;
use crate::admission_retry::retry_until_with_remaining;
use crate::protocol::emit;
use crate::protocol_admin_result::{
    DescribedTopicResult, described_partitions, listed_consumer_group_offset, listed_offset,
    listed_topics,
};
use crate::state::AdapterState;

pub(crate) fn dispatch<W: Write>(
    state: &AdapterState,
    writer: &mut W,
    command_id: CommandId,
    command: AdapterCommand,
) -> Result<(), AdapterError> {
    match command {
        AdapterCommand::DescribeTopic(command) => {
            describe_topic(state, writer, command_id, command)
        }
        AdapterCommand::ListTopics(command) => list_topics(state, writer, command_id, command),
        AdapterCommand::ListOffsets(command) => list_offset(state, writer, command_id, command),
        AdapterCommand::ListConsumerGroupOffsets(command) => {
            list_consumer_group_offset(state, writer, command_id, command)
        }
        _ => Err(AdapterError::AdminResult(
            "non-admin-read command reached admin read dispatcher".to_owned(),
        )),
    }
}

fn describe_topic<W: Write>(
    state: &AdapterState,
    writer: &mut W,
    command_id: CommandId,
    command: DescribeTopicCommand,
) -> Result<(), AdapterError> {
    let deadline = deadline_after(command.timeout_ms);
    let client = state.client(&command.client_id)?;
    let result = retry_until_with_remaining(
        deadline,
        |remaining| {
            client
                .admin()
                .describe_topics([command.topic.clone()])
                .deadline_after(remaining)
                .submit()
                .wait()
        },
        retry_safe,
    )
    .map_err(AdapterError::Client)?;
    let entries = result
        .into_entries()
        .into_iter()
        .map(|(key, result)| (key, result.map(DescribedTopicResult::from)))
        .collect();
    let partitions = described_partitions(entries, &command.operation_id, &command.topic)?;
    emit_event(
        writer,
        command_id,
        AdapterEvent::TopicDescribed(AdminTopicDescription {
            operation_id: command.operation_id,
            topic: command.topic,
            partitions,
        }),
    )
}

fn list_topics<W: Write>(
    state: &AdapterState,
    writer: &mut W,
    command_id: CommandId,
    command: ListTopicsCommand,
) -> Result<(), AdapterError> {
    let result = state
        .client(&command.client_id)?
        .admin()
        .list_topics()
        .include_internal(command.include_internal)
        .deadline_after(Duration::from_millis(command.timeout_ms))
        .submit()
        .wait()
        .map_err(AdapterError::Client)?;
    let entries = result
        .into_entries()
        .into_iter()
        .map(|(key, result)| (key, result.map(|value| value.name().to_owned())))
        .collect();
    let topics = listed_topics(entries, &command.operation_id)?;
    emit_event(
        writer,
        command_id,
        AdapterEvent::TopicsListed(AdminTopicsListing {
            operation_id: command.operation_id,
            topics,
        }),
    )
}

fn list_offset<W: Write>(
    state: &AdapterState,
    writer: &mut W,
    command_id: CommandId,
    command: ListOffsetsCommand,
) -> Result<(), AdapterError> {
    let deadline = deadline_after(command.timeout_ms);
    let client = state.client(&command.client_id)?;
    let result = retry_until_with_remaining(
        deadline,
        |remaining| {
            let spec = offset_spec(command.position);
            let query = ListOffsetsQuery::new(command.topic.clone(), command.partition, spec);
            client
                .admin()
                .list_offsets([query])
                .read_isolation(ReadIsolation::ReadCommitted)
                .deadline_after(remaining)
                .submit()
                .wait()
        },
        retry_safe,
    )
    .map_err(AdapterError::Client)?;
    let entries = result
        .into_offsets()
        .into_entries()
        .into_iter()
        .map(|(key, result)| (key, result.map(|value| value.offset())))
        .collect();
    let offset = listed_offset(
        entries,
        &command.operation_id,
        &command.topic,
        command.partition,
    )?;
    emit_event(
        writer,
        command_id,
        AdapterEvent::OffsetListed(AdminOffsetListing {
            operation_id: command.operation_id,
            topic: command.topic,
            partition: command.partition,
            offset,
        }),
    )
}

pub(crate) const fn offset_spec(position: AdminOffsetPosition) -> OffsetSpec {
    match position {
        AdminOffsetPosition::Earliest => OffsetSpec::earliest(),
        AdminOffsetPosition::Latest => OffsetSpec::latest(),
    }
}

fn list_consumer_group_offset<W: Write>(
    state: &AdapterState,
    writer: &mut W,
    command_id: CommandId,
    command: ListConsumerGroupOffsetsCommand,
) -> Result<(), AdapterError> {
    let result = state
        .client(&command.client_id)?
        .admin()
        .list_consumer_group_offsets(command.group_id.clone())
        .partitions([TopicPartition::new(
            command.topic.clone(),
            command.partition,
        )])
        .require_stable(command.require_stable)
        .deadline_after(Duration::from_millis(command.timeout_ms))
        .submit()
        .wait()
        .map_err(AdapterError::Client)?;
    let entries = result
        .into_offsets()
        .into_entries()
        .into_iter()
        .map(|(key, result)| (key, result.map(|value| value.committed_offset())))
        .collect();
    let offset = listed_consumer_group_offset(
        entries,
        &command.operation_id,
        &command.topic,
        command.partition,
    )?;
    emit_event(
        writer,
        command_id,
        AdapterEvent::ConsumerGroupOffsetListed(AdminConsumerGroupOffsetListing {
            operation_id: command.operation_id,
            group_id: command.group_id,
            topic: command.topic,
            partition: command.partition,
            offset,
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

fn emit_event<W: Write>(
    writer: &mut W,
    command_id: CommandId,
    event: AdapterEvent,
) -> Result<(), AdapterError> {
    emit(writer, &AdapterEventEnvelope::new(command_id, event))
}
