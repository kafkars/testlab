//! Share-group Admin operations retain public description and partition-offset facts.

use std::io::Write;
use std::time::{Duration, Instant};

use testlab_schema::{
    AdapterEvent, AdapterEventEnvelope, AdminShareGroupDescription, AdminShareGroupMember,
    AdminShareGroupOffsetAlteration, AdminShareGroupOffsetListing, AdminShareGroupTopicAssignment,
    AlterShareGroupOffsetsCommand, CommandId, DescribeShareGroupCommand,
    ListShareGroupOffsetsCommand,
};

use crate::AdapterError;
use crate::admission_retry::retry_until_with_remaining;
use crate::kafkars_api::{
    KafkaError, RetryAdvice, ShareGroupDescription, ShareGroupOffsetAlteration, TopicPartition,
};
use crate::protocol::emit;
use crate::protocol_admin_plural_result::{ResourceResult, ordered_partition_results};
use crate::state::AdapterState;

pub(crate) fn describe<W: Write>(
    state: &AdapterState,
    writer: &mut W,
    command_id: CommandId,
    command: DescribeShareGroupCommand,
) -> Result<(), AdapterError> {
    let deadline = deadline_after(command.timeout_ms);
    let client = state.client(&command.client_id)?;
    let result = retry_until_with_remaining(
        deadline,
        |remaining| {
            client
                .admin()
                .describe_share_group(command.group_id.clone())
                .include_authorized_operations(command.include_authorized_operations)
                .deadline_after(remaining)
                .submit()
                .wait()
        },
        retry_safe,
    )
    .map_err(AdapterError::Client)?;
    let description = public_description(command.operation_id, &result.into_description());
    emit(
        writer,
        &AdapterEventEnvelope::new(command_id, AdapterEvent::ShareGroupDescribed(description)),
    )?;
    Ok(())
}

pub(crate) fn list_offsets<W: Write>(
    state: &AdapterState,
    writer: &mut W,
    command_id: CommandId,
    command: ListShareGroupOffsetsCommand,
) -> Result<(), AdapterError> {
    let result = state
        .client(&command.client_id)?
        .admin()
        .list_share_group_offsets(command.group_id.clone())
        .partitions([TopicPartition::new(
            command.topic.clone(),
            command.partition,
        )])
        .deadline_after(Duration::from_millis(command.timeout_ms))
        .submit()
        .wait()
        .map_err(AdapterError::Client)?;
    let requested = [(command.topic.clone(), command.partition)];
    let result = ordered_partition_results(
        result.into_offsets().into_entries(),
        &requested,
        &command.operation_id,
        "Share-group offset",
    )?
    .into_iter()
    .next()
    .ok_or_else(|| {
        AdapterError::AdminResult(format!(
            "admin operation {} omitted its Share-group offset outcome",
            command.operation_id
        ))
    })?;
    let (topic_id, start_offset, leader_epoch, lag, error_code) = match result.result {
        ResourceResult::Success(offset) => (
            offset.topic_id(),
            offset.start_offset(),
            offset.leader_epoch(),
            offset.lag(),
            None,
        ),
        ResourceResult::Failure(code) => ([0; 16], None, None, None, Some(code)),
    };
    emit(
        writer,
        &AdapterEventEnvelope::new(
            command_id,
            AdapterEvent::ShareGroupOffsetsListed(AdminShareGroupOffsetListing {
                operation_id: command.operation_id,
                group_id: command.group_id,
                topic: result.topic,
                partition: result.partition,
                topic_id,
                start_offset,
                leader_epoch,
                lag,
                error_code,
            }),
        ),
    )
}

pub(crate) fn alter_offsets<W: Write>(
    state: &AdapterState,
    writer: &mut W,
    command_id: CommandId,
    command: AlterShareGroupOffsetsCommand,
) -> Result<(), AdapterError> {
    let result = state
        .client(&command.client_id)?
        .admin()
        .alter_share_group_offsets(
            command.group_id.clone(),
            [ShareGroupOffsetAlteration::new(
                command.topic.clone(),
                command.partition,
                command.start_offset,
            )],
        )
        .deadline_after(Duration::from_millis(command.timeout_ms))
        .submit()
        .wait()
        .map_err(AdapterError::Client)?;
    let requested = [(command.topic.clone(), command.partition)];
    let result = ordered_partition_results(
        result.into_offsets().into_entries(),
        &requested,
        &command.operation_id,
        "Share-group offset alteration",
    )?
    .into_iter()
    .next()
    .ok_or_else(|| {
        AdapterError::AdminResult(format!(
            "admin operation {} omitted its Share-group offset alteration outcome",
            command.operation_id
        ))
    })?;
    let (topic_id, error_code) = match result.result {
        ResourceResult::Success(topic_id) => (topic_id, None),
        ResourceResult::Failure(code) => ([0; 16], Some(code)),
    };
    emit(
        writer,
        &AdapterEventEnvelope::new(
            command_id,
            AdapterEvent::ShareGroupOffsetsAltered(AdminShareGroupOffsetAlteration {
                operation_id: command.operation_id,
                group_id: command.group_id,
                topic: result.topic,
                partition: result.partition,
                topic_id,
                error_code,
            }),
        ),
    )
}

pub(crate) fn public_description(
    operation_id: testlab_schema::OperationId,
    description: &ShareGroupDescription,
) -> AdminShareGroupDescription {
    let members = description
        .members()
        .iter()
        .map(|member| AdminShareGroupMember {
            member_id: member.member_id().to_owned(),
            rack_id: member.rack_id().map(str::to_owned),
            member_epoch: member.member_epoch(),
            client_id: member.client_id().to_owned(),
            subscribed_topics: member.subscribed_topic_names().to_vec(),
            assignments: member
                .assignment()
                .topics()
                .iter()
                .map(|assignment| AdminShareGroupTopicAssignment {
                    topic_id: assignment.topic_id(),
                    topic: assignment.topic_name().to_owned(),
                    partitions: assignment.partitions().to_vec(),
                })
                .collect(),
        })
        .collect();
    AdminShareGroupDescription {
        operation_id,
        group_id: description.group_id().to_owned(),
        state: description.state().to_owned(),
        group_epoch: description.group_epoch(),
        assignment_epoch: description.assignment_epoch(),
        assignor_name: description.assignor_name().to_owned(),
        authorized_operations: description.authorized_operations(),
        members,
    }
}

pub(crate) fn deadline_after(timeout_ms: u64) -> Instant {
    let started = Instant::now();
    started
        .checked_add(Duration::from_millis(timeout_ms))
        .unwrap_or(started)
}

pub(crate) fn retry_safe(error: &KafkaError) -> bool {
    error.retry_advice() == RetryAdvice::RetrySafe
}
