//! Session terminals distinguish clean lifecycle completion from failure abort.

use std::io::Write;

use testlab_schema::{AdapterCommand, AdapterEvent, AdapterEventEnvelope, CommandId};

use crate::session::{AdapterError, emit};
use crate::state::AdapterState;

pub(crate) fn dispatch<W: Write>(
    state: &AdapterState,
    writer: &mut W,
    command_id: CommandId,
    command: &AdapterCommand,
) -> Result<bool, AdapterError> {
    let event = match command {
        AdapterCommand::Finish => {
            state.finish()?;
            AdapterEvent::Finished
        }
        AdapterCommand::Abort => AdapterEvent::Aborted,
        _ => {
            return Err(AdapterError::State(
                "non-terminal command reached session end".to_owned(),
            ));
        }
    };
    emit(writer, &AdapterEventEnvelope::new(command_id, event))?;
    Ok(true)
}
