//! Public client failures remain distinct from fatal adapter failures.

use std::io::Write;

use testlab_schema::{AdapterEvent, AdapterEventEnvelope, CommandEnvelope};

use crate::protocol::emit;
use crate::{AdapterError, normalize};

pub(super) fn emit_client_failure<W: Write>(
    writer: &mut W,
    envelope: CommandEnvelope,
    error: AdapterError,
) -> Result<(), AdapterError> {
    let Some(client_error) = error.client_failure() else {
        return Err(error);
    };
    eprintln!(
        "Kafkars command {} failed: {client_error}",
        envelope.command_id
    );
    emit(
        writer,
        &AdapterEventEnvelope::new(
            envelope.command_id,
            AdapterEvent::CommandFailed {
                code: normalize::error_code(client_error),
                diagnostic: client_error.to_string(),
            },
        ),
    )
}
