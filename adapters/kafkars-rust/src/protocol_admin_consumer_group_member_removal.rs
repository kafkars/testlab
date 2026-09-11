//! Static consumer-group member removal preserves reason, throttle, order, and partial errors.

use std::io::Write;

use testlab_schema::{
    AdapterEvent, AdapterEventEnvelope, AdminConsumerGroupMemberRemovalOutcome,
    AdminConsumerGroupMembersRemoval, CommandId, OperationId, RemoveConsumerGroupMembersCommand,
};

use crate::AdapterError;
use crate::admission_retry::retry_until_with_remaining;
use crate::kafkars_api::{ConsumerGroupMemberRemoval, KafkaError};
use crate::protocol::emit;
use crate::protocol_admin_read::{deadline_after, retry_safe};
use crate::state::AdapterState;

pub(crate) fn remove<W: Write>(
    state: &AdapterState,
    writer: &mut W,
    command_id: CommandId,
    command: RemoveConsumerGroupMembersCommand,
) -> Result<(), AdapterError> {
    let deadline = deadline_after(command.timeout_ms);
    let client = state.client(&command.client_id)?;
    let result = retry_until_with_remaining(
        deadline,
        |remaining| {
            client
                .admin()
                .remove_consumer_group_members(
                    command.group_id.clone(),
                    command
                        .group_instance_ids
                        .iter()
                        .cloned()
                        .map(ConsumerGroupMemberRemoval::new),
                )
                .reason(command.reason.clone())
                .deadline_after(remaining)
                .submit()
                .wait()
        },
        retry_safe,
    )
    .map_err(AdapterError::Client)?;
    let throttle_time_ms = u64::try_from(result.throttle_time().as_millis()).map_err(|_| {
        invalid(
            &command.operation_id,
            "returned an unrepresentable throttle time",
        )
    })?;
    let outcomes = outcomes(
        result.into_members().into_entries(),
        &command.group_instance_ids,
        &command.operation_id,
    )?;
    emit(
        writer,
        &AdapterEventEnvelope::new(
            command_id,
            AdapterEvent::ConsumerGroupMembersRemoved(AdminConsumerGroupMembersRemoval {
                operation_id: command.operation_id,
                group_id: command.group_id,
                throttle_time_ms,
                outcomes,
            }),
        ),
    )
}

pub(crate) fn outcomes(
    entries: Vec<(String, Result<(), KafkaError>)>,
    expected: &[String],
    operation_id: &OperationId,
) -> Result<Vec<AdminConsumerGroupMemberRemovalOutcome>, AdapterError> {
    if entries.len() != expected.len() {
        return Err(invalid(
            operation_id,
            "returned a different number of static-member outcomes than requested",
        ));
    }
    entries
        .into_iter()
        .zip(expected)
        .map(|((group_instance_id, result), expected)| {
            if &group_instance_id != expected {
                return Err(invalid(
                    operation_id,
                    "returned static-member outcomes outside caller order",
                ));
            }
            Ok(AdminConsumerGroupMemberRemovalOutcome {
                group_instance_id,
                error_code: result
                    .err()
                    .map(|error| crate::normalize::error_code(&error)),
            })
        })
        .collect()
}

fn invalid(operation_id: &OperationId, detail: &str) -> AdapterError {
    AdapterError::AdminResult(format!("admin operation {operation_id} {detail}"))
}
