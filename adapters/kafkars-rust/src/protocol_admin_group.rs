//! Consumer-group admin commands retain exact group identities and partial errors.

use std::io::Write;
use std::time::{Duration, Instant};

use crate::kafkars_api::{KafkaError, RetryAdvice};
use testlab_schema::{
    AdapterCommand, AdapterEvent, AdapterEventEnvelope, AdminBrokerError,
    AdminConsumerGroupCompletion, AdminConsumerGroupDescription, AdminConsumerGroupsListing,
    CommandId, DeleteConsumerGroupCommand, DescribeConsumerGroupCommand, ListConsumerGroupsCommand,
};

use crate::AdapterError;
use crate::admission_retry::retry_until_with_remaining;
use crate::protocol::emit;
use crate::protocol_admin_group_offset_mutation;
use crate::protocol_admin_result::{
    sorted_unique_broker_errors, sorted_unique_strings, take_single_result,
};
use crate::state::AdapterState;

pub(crate) fn dispatch<W: Write>(
    state: &AdapterState,
    writer: &mut W,
    command_id: CommandId,
    command: AdapterCommand,
) -> Result<(), AdapterError> {
    match command {
        AdapterCommand::ListConsumerGroups(command) => list(state, writer, command_id, command),
        AdapterCommand::DescribeConsumerGroup(command) => {
            describe(state, writer, command_id, command)
        }
        AdapterCommand::AlterConsumerGroupOffset(command) => {
            protocol_admin_group_offset_mutation::alter(state, writer, command_id, command)
        }
        AdapterCommand::DeleteConsumerGroupOffset(command) => {
            protocol_admin_group_offset_mutation::delete(state, writer, command_id, command)
        }
        AdapterCommand::DeleteConsumerGroup(command) => delete(state, writer, command_id, command),
        _ => Err(AdapterError::AdminResult(
            "non-consumer-group command reached group admin dispatcher".to_owned(),
        )),
    }
}

fn list<W: Write>(
    state: &AdapterState,
    writer: &mut W,
    command_id: CommandId,
    command: ListConsumerGroupsCommand,
) -> Result<(), AdapterError> {
    let deadline = deadline_after(command.timeout_ms);
    let client = state.client(&command.client_id)?;
    let result = retry_until_with_remaining(
        deadline,
        |remaining| {
            client
                .admin()
                .list_consumer_groups()
                .deadline_after(remaining)
                .submit()
                .wait()
        },
        retry_safe,
    )
    .map_err(AdapterError::Client)?;
    let (_, groups, broker_errors) = result.into_parts();
    let group_ids = groups
        .into_iter()
        .map(|group| group.group_id().to_owned())
        .collect();
    let group_ids =
        sorted_unique_strings(group_ids, &command.operation_id, "consumer-group listing")?;
    let broker_errors = broker_errors
        .into_iter()
        .map(|error| AdminBrokerError {
            broker_id: error.broker_id(),
            code: error.code(),
        })
        .collect();
    let broker_errors = sorted_unique_broker_errors(broker_errors, &command.operation_id)?;
    emit_event(
        writer,
        command_id,
        AdapterEvent::ConsumerGroupsListed(AdminConsumerGroupsListing {
            operation_id: command.operation_id,
            group_ids,
            broker_errors,
        }),
    )
}

fn describe<W: Write>(
    state: &AdapterState,
    writer: &mut W,
    command_id: CommandId,
    command: DescribeConsumerGroupCommand,
) -> Result<(), AdapterError> {
    let deadline = deadline_after(command.timeout_ms);
    let client = state.client(&command.client_id)?;
    let result = retry_until_with_remaining(
        deadline,
        |remaining| {
            client
                .admin()
                .describe_consumer_groups([command.group_id.clone()])
                .include_authorized_operations(false)
                .deadline_after(remaining)
                .submit()
                .wait()
        },
        retry_safe,
    )
    .map_err(AdapterError::Client)?;
    let description = take_single_result(
        result.into_groups().into_entries(),
        &command.operation_id,
        |group_id| group_id == &command.group_id,
        "consumer-group description",
    )?;
    let member_count = u32::try_from(description.members().len()).map_err(|_| {
        AdapterError::AdminResult(format!(
            "admin operation {} returned too many consumer-group members",
            command.operation_id
        ))
    })?;
    emit_event(
        writer,
        command_id,
        AdapterEvent::ConsumerGroupDescribed(AdminConsumerGroupDescription {
            operation_id: command.operation_id,
            group_id: command.group_id,
            member_count,
        }),
    )
}

fn delete<W: Write>(
    state: &AdapterState,
    writer: &mut W,
    command_id: CommandId,
    command: DeleteConsumerGroupCommand,
) -> Result<(), AdapterError> {
    let deadline = deadline_after(command.timeout_ms);
    let client = state.client(&command.client_id)?;
    let result = retry_until_with_remaining(
        deadline,
        |remaining| {
            client
                .admin()
                .delete_consumer_groups([command.group_id.clone()])
                .deadline_after(remaining)
                .submit()
                .wait()
        },
        retry_safe,
    )
    .map_err(AdapterError::Client)?;
    take_single_result(
        result.into_groups().into_entries(),
        &command.operation_id,
        |group_id| group_id == &command.group_id,
        "consumer-group deletion",
    )?;
    emit_event(
        writer,
        command_id,
        AdapterEvent::ConsumerGroupDeleted(AdminConsumerGroupCompletion {
            operation_id: command.operation_id,
            group_id: command.group_id,
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
