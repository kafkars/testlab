//! Consumer-group batch deletion preserves public caller order and every outcome.

use std::io::Write;

use testlab_schema::{
    AdapterEvent, AdapterEventEnvelope, AdminConsumerGroupDeletionOutcome,
    AdminConsumerGroupsDeletion, CommandId, DeleteConsumerGroupsCommand, OperationId,
};

use crate::AdapterError;
use crate::admission_retry::retry_until_with_remaining;
use crate::kafkars_api::KafkaError;
use crate::normalize;
use crate::protocol::emit;
use crate::protocol_admin_read::{deadline_after, retry_safe};
use crate::state::AdapterState;

pub(crate) fn delete<W: Write>(
    state: &AdapterState,
    writer: &mut W,
    command_id: CommandId,
    command: DeleteConsumerGroupsCommand,
) -> Result<(), AdapterError> {
    let deadline = deadline_after(command.timeout_ms);
    let client = state.client(&command.client_id)?;
    let result = retry_until_with_remaining(
        deadline,
        |remaining| {
            client
                .admin()
                .delete_consumer_groups(command.group_ids.clone())
                .deadline_after(remaining)
                .submit()
                .wait()
        },
        retry_safe,
    )
    .map_err(AdapterError::Client)?;
    let outcomes = outcomes(
        result.into_groups().into_entries(),
        &command.group_ids,
        &command.operation_id,
    )?;
    emit(
        writer,
        &AdapterEventEnvelope::new(
            command_id,
            AdapterEvent::ConsumerGroupsDeleted(AdminConsumerGroupsDeletion {
                operation_id: command.operation_id,
                outcomes,
            }),
        ),
    )
}

pub(crate) fn outcomes(
    entries: Vec<(String, Result<(), KafkaError>)>,
    expected: &[String],
    operation_id: &OperationId,
) -> Result<Vec<AdminConsumerGroupDeletionOutcome>, AdapterError> {
    if entries.len() != expected.len() {
        return Err(invalid(
            operation_id,
            "returned a different number of consumer-group outcomes than requested",
        ));
    }
    entries
        .into_iter()
        .zip(expected)
        .map(|((group_id, result), expected)| {
            if &group_id != expected {
                return Err(invalid(
                    operation_id,
                    "returned consumer-group outcomes outside caller order",
                ));
            }
            Ok(AdminConsumerGroupDeletionOutcome {
                group_id,
                error_code: result.err().map(|error| normalize::error_code(&error)),
            })
        })
        .collect()
}

fn invalid(operation_id: &OperationId, detail: &str) -> AdapterError {
    AdapterError::AdminResult(format!("admin operation {operation_id} {detail}"))
}
