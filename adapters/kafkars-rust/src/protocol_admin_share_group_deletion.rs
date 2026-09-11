//! Share-group deletion retains caller order and every public per-group outcome.

use std::io::Write;
use std::time::Duration;

use testlab_schema::{
    AdapterEvent, AdapterEventEnvelope, AdminShareGroupDeletionOutcome, AdminShareGroupsDeletion,
    CommandId, DeleteShareGroupsCommand,
};

use crate::protocol_admin_plural_result::{ResourceResult, ordered_group_results};
use crate::state::AdapterState;
use crate::{AdapterError, protocol};

pub(crate) fn delete<W: Write>(
    state: &AdapterState,
    writer: &mut W,
    command_id: CommandId,
    command: DeleteShareGroupsCommand,
) -> Result<(), AdapterError> {
    let requested = command.group_ids.clone();
    let result = state
        .client(&command.client_id)?
        .admin()
        .delete_share_groups(requested.clone())
        .deadline_after(Duration::from_millis(command.timeout_ms))
        .submit()
        .wait()
        .map_err(AdapterError::Client)?;
    let outcomes = ordered_group_results(
        result.into_groups().into_entries(),
        &requested,
        &command.operation_id,
        "Share-group deletion",
    )?
    .into_iter()
    .map(|outcome| AdminShareGroupDeletionOutcome {
        group_id: outcome.group_id,
        error_code: match outcome.result {
            ResourceResult::Success(()) => None,
            ResourceResult::Failure(code) => Some(code),
        },
    })
    .collect();
    protocol::emit(
        writer,
        &AdapterEventEnvelope::new(
            command_id,
            AdapterEvent::ShareGroupsDeleted(AdminShareGroupsDeletion {
                operation_id: command.operation_id,
                outcomes,
            }),
        ),
    )
}
