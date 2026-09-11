//! Share-group Admin description retains public group and assignment facts.

use std::io::Write;
use std::time::{Duration, Instant};

use testlab_schema::{
    AdapterEvent, AdapterEventEnvelope, AdminShareGroupDescription, AdminShareGroupMember,
    AdminShareGroupTopicAssignment, CommandId, DescribeShareGroupCommand,
};

use crate::AdapterError;
use crate::admission_retry::retry_until_with_remaining;
use crate::kafkars_api::{KafkaError, RetryAdvice, ShareGroupDescription};
use crate::protocol::emit;
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
                .include_authorized_operations(false)
                .deadline_after(remaining)
                .submit()
                .wait()
        },
        retry_safe,
    )
    .map_err(AdapterError::Client)?;
    let description = public_description(command, result.into_description());
    emit(
        writer,
        &AdapterEventEnvelope::new(command_id, AdapterEvent::ShareGroupDescribed(description)),
    )?;
    Ok(())
}

fn public_description(
    command: DescribeShareGroupCommand,
    description: ShareGroupDescription,
) -> AdminShareGroupDescription {
    let members = description
        .members()
        .iter()
        .map(|member| AdminShareGroupMember {
            member_id: member.member_id().to_owned(),
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
        operation_id: command.operation_id,
        group_id: description.group_id().to_owned(),
        state: description.state().to_owned(),
        group_epoch: description.group_epoch(),
        assignment_epoch: description.assignment_epoch(),
        assignor_name: description.assignor_name().to_owned(),
        members,
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
