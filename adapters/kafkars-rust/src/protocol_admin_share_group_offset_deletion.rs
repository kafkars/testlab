//! Share-group offset deletion preserves one exact public per-topic outcome.

use std::io::Write;
use std::time::Duration;

use testlab_schema::{
    AdapterEvent, AdapterEventEnvelope, AdminShareGroupOffsetDeletion, CommandId,
    DeleteShareGroupOffsetsCommand,
};

use crate::state::AdapterState;
use crate::{AdapterError, normalize};

pub(crate) fn delete<W: Write>(
    state: &AdapterState,
    writer: &mut W,
    command_id: CommandId,
    command: DeleteShareGroupOffsetsCommand,
) -> Result<(), AdapterError> {
    let result = state
        .client(&command.client_id)?
        .admin()
        .delete_share_group_offsets(command.group_id.clone(), [command.topic.clone()])
        .deadline_after(Duration::from_millis(command.timeout_ms))
        .submit()
        .wait()
        .map_err(AdapterError::Client)?;
    let mut entries = result.into_topics().into_entries();
    if entries.len() != 1 {
        return Err(invalid(
            &command,
            "returned a different number of topic outcomes than requested",
        ));
    }
    let Some((topic, result)) = entries.pop() else {
        return Err(invalid(&command, "omitted its topic outcome"));
    };
    if topic != command.topic {
        return Err(invalid(&command, "returned a mismatched topic identity"));
    }
    let (topic_id, error_code) = match result {
        Ok(topic_id) => (topic_id, None),
        Err(error) => ([0; 16], Some(normalize::error_code(&error))),
    };
    emit(
        writer,
        command_id,
        AdminShareGroupOffsetDeletion {
            operation_id: command.operation_id,
            group_id: command.group_id,
            topic,
            topic_id,
            error_code,
        },
    )
}

fn emit<W: Write>(
    writer: &mut W,
    command_id: CommandId,
    deletion: AdminShareGroupOffsetDeletion,
) -> Result<(), AdapterError> {
    crate::protocol::emit(
        writer,
        &AdapterEventEnvelope::new(command_id, AdapterEvent::ShareGroupOffsetsDeleted(deletion)),
    )
}

fn invalid(command: &DeleteShareGroupOffsetsCommand, detail: &str) -> AdapterError {
    AdapterError::AdminResult(format!(
        "admin operation {} Share-group offset deletion {detail}",
        command.operation_id
    ))
}
