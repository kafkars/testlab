//! Session output is serialized and flushed at every protocol boundary.

use crate::AdapterError;
use std::io::Write;
use testlab_schema::{AdapterEvent, AdapterEventEnvelope, CommandEnvelope};

pub(crate) fn emit<W: Write>(
    writer: &mut W,
    event: &AdapterEventEnvelope,
) -> Result<(), AdapterError> {
    serde_json::to_writer(&mut *writer, event)?;
    writer.write_all(b"\n")?;
    writer.flush()?;
    Ok(())
}

pub(crate) fn emit_fatal<W: Write>(
    writer: &mut W,
    envelope: CommandEnvelope,
    error: AdapterError,
) -> Result<(), AdapterError> {
    let event = AdapterEventEnvelope::new(
        envelope.command_id,
        AdapterEvent::Fatal {
            code: error.code().to_owned(),
            diagnostic: error.to_string(),
        },
    );
    let _ = emit(writer, &event);
    Err(error)
}
