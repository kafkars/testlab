//! Admin command routing keeps public read and write operations separate.

use std::io::Write;

use testlab_schema::{AdapterCommand, CommandId};

use crate::AdapterError;
use crate::protocol_admin_read;
use crate::protocol_admin_write;
use crate::state::AdapterState;

pub(crate) fn dispatch<W: Write>(
    state: &AdapterState,
    writer: &mut W,
    command_id: CommandId,
    command: AdapterCommand,
) -> Result<(), AdapterError> {
    match command {
        command
        @ (AdapterCommand::CreateTopic { .. } | AdapterCommand::CreatePartitions { .. }) => {
            protocol_admin_write::dispatch(state, writer, command_id, command)
        }
        command @ (AdapterCommand::DescribeTopic { .. }
        | AdapterCommand::ListTopics { .. }
        | AdapterCommand::ListOffsets { .. }) => {
            protocol_admin_read::dispatch(state, writer, command_id, command)
        }
        _ => Err(AdapterError::AdminResult(
            "non-admin command reached admin dispatcher".to_owned(),
        )),
    }
}
