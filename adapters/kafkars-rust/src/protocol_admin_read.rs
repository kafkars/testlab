//! Read-only admin commands preserve exact packaged public result identities.

use std::io::Write;
use std::time::{Duration, Instant};

use kafkars::{ListOffsetsQuery, OffsetSpec, ReadIsolation, RetryAdvice, TopicPartition};
use testlab_schema::{
    AdapterCommand, AdapterEvent, AdapterEventEnvelope, AdminOffsetPosition, ClientId, CommandId,
    OperationId,
};

use crate::AdapterError;
use crate::admission_retry::retry_until_with_remaining;
use crate::protocol::emit;
use crate::protocol_admin_result::{
    DescribedTopicResult, described_partitions, listed_consumer_group_offset, listed_offset,
    listed_topics,
};
use crate::state::AdapterState;

struct ListTopicsInput {
    client_id: ClientId,
    operation_id: OperationId,
    include_internal: bool,
    timeout_ms: u64,
}

struct ListOffsetInput {
    client_id: ClientId,
    operation_id: OperationId,
    topic: String,
    partition: i32,
    position: AdminOffsetPosition,
    timeout_ms: u64,
}

struct ListConsumerGroupOffsetInput {
    client_id: ClientId,
    operation_id: OperationId,
    group_id: String,
    topic: String,
    partition: i32,
    require_stable: bool,
    timeout_ms: u64,
}

pub(crate) fn dispatch<W: Write>(
    state: &AdapterState,
    writer: &mut W,
    command_id: CommandId,
    command: AdapterCommand,
) -> Result<(), AdapterError> {
    match command {
        AdapterCommand::DescribeTopic {
            client_id,
            operation_id,
            topic,
            timeout_ms,
        } => describe_topic(
            state,
            writer,
            command_id,
            &client_id,
            operation_id,
            topic,
            timeout_ms,
        ),
        AdapterCommand::ListTopics {
            client_id,
            operation_id,
            include_internal,
            timeout_ms,
        } => list_topics(
            state,
            writer,
            command_id,
            ListTopicsInput {
                client_id,
                operation_id,
                include_internal,
                timeout_ms,
            },
        ),
        AdapterCommand::ListOffsets {
            client_id,
            operation_id,
            topic,
            partition,
            position,
            timeout_ms,
        } => list_offset(
            state,
            writer,
            command_id,
            ListOffsetInput {
                client_id,
                operation_id,
                topic,
                partition,
                position,
                timeout_ms,
            },
        ),
        AdapterCommand::ListConsumerGroupOffsets(input) => list_consumer_group_offset(
            state,
            writer,
            command_id,
            ListConsumerGroupOffsetInput {
                client_id: input.client_id,
                operation_id: input.operation_id,
                group_id: input.group_id,
                topic: input.topic,
                partition: input.partition,
                require_stable: input.require_stable,
                timeout_ms: input.timeout_ms,
            },
        ),
        _ => Err(AdapterError::AdminResult(
            "non-admin-read command reached admin read dispatcher".to_owned(),
        )),
    }
}

fn describe_topic<W: Write>(
    state: &AdapterState,
    writer: &mut W,
    command_id: CommandId,
    client_id: &ClientId,
    operation_id: OperationId,
    topic: String,
    timeout_ms: u64,
) -> Result<(), AdapterError> {
    let result = state
        .client(client_id)?
        .admin()
        .describe_topics([topic.clone()])
        .deadline_after(Duration::from_millis(timeout_ms))
        .submit()
        .wait()
        .map_err(AdapterError::Client)?;
    let entries = result
        .into_entries()
        .into_iter()
        .map(|(key, result)| (key, result.map(DescribedTopicResult::from)))
        .collect();
    let partitions = described_partitions(entries, &operation_id, &topic)?;
    emit(
        writer,
        &AdapterEventEnvelope::new(
            command_id,
            AdapterEvent::TopicDescribed {
                operation_id,
                topic,
                partitions,
            },
        ),
    )
}

fn list_topics<W: Write>(
    state: &AdapterState,
    writer: &mut W,
    command_id: CommandId,
    input: ListTopicsInput,
) -> Result<(), AdapterError> {
    let result = state
        .client(&input.client_id)?
        .admin()
        .list_topics()
        .include_internal(input.include_internal)
        .deadline_after(Duration::from_millis(input.timeout_ms))
        .submit()
        .wait()
        .map_err(AdapterError::Client)?;
    let entries = result
        .into_entries()
        .into_iter()
        .map(|(key, result)| (key, result.map(|value| value.name().to_owned())))
        .collect();
    let topics = listed_topics(entries, &input.operation_id)?;
    emit(
        writer,
        &AdapterEventEnvelope::new(
            command_id,
            AdapterEvent::TopicsListed {
                operation_id: input.operation_id,
                topics,
            },
        ),
    )
}

fn list_offset<W: Write>(
    state: &AdapterState,
    writer: &mut W,
    command_id: CommandId,
    input: ListOffsetInput,
) -> Result<(), AdapterError> {
    let started = Instant::now();
    let deadline = started
        .checked_add(Duration::from_millis(input.timeout_ms))
        .unwrap_or(started);
    let client = state.client(&input.client_id)?;
    let result = retry_until_with_remaining(
        deadline,
        |remaining| {
            let spec = match input.position {
                AdminOffsetPosition::Latest => OffsetSpec::latest(),
            };
            let query = ListOffsetsQuery::new(input.topic.clone(), input.partition, spec);
            client
                .admin()
                .list_offsets([query])
                .read_isolation(ReadIsolation::ReadCommitted)
                .deadline_after(remaining)
                .submit()
                .wait()
        },
        |error| error.retry_advice() == RetryAdvice::RetrySafe,
    )
    .map_err(AdapterError::Client)?;
    let entries = result
        .into_offsets()
        .into_entries()
        .into_iter()
        .map(|(key, result)| (key, result.map(|value| value.offset())))
        .collect();
    let offset = listed_offset(entries, &input.operation_id, &input.topic, input.partition)?;
    emit(
        writer,
        &AdapterEventEnvelope::new(
            command_id,
            AdapterEvent::OffsetListed {
                operation_id: input.operation_id,
                topic: input.topic,
                partition: input.partition,
                offset,
            },
        ),
    )
}

fn list_consumer_group_offset<W: Write>(
    state: &AdapterState,
    writer: &mut W,
    command_id: CommandId,
    input: ListConsumerGroupOffsetInput,
) -> Result<(), AdapterError> {
    // The singular public operation owns the group identity; its result keys
    // independently echo only the selected topic-partition.
    let result = state
        .client(&input.client_id)?
        .admin()
        .list_consumer_group_offsets(input.group_id.clone())
        .partitions([TopicPartition::new(input.topic.clone(), input.partition)])
        .require_stable(input.require_stable)
        .deadline_after(Duration::from_millis(input.timeout_ms))
        .submit()
        .wait()
        .map_err(AdapterError::Client)?;
    let entries = result
        .into_offsets()
        .into_entries()
        .into_iter()
        .map(|(key, result)| (key, result.map(|value| value.committed_offset())))
        .collect();
    let offset =
        listed_consumer_group_offset(entries, &input.operation_id, &input.topic, input.partition)?;
    emit(
        writer,
        &AdapterEventEnvelope::new(
            command_id,
            AdapterEvent::ConsumerGroupOffsetListed {
                operation_id: input.operation_id,
                group_id: input.group_id,
                topic: input.topic,
                partition: input.partition,
                offset,
            },
        ),
    )
}
