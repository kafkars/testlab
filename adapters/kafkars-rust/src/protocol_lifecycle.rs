//! Lifecycle commands settle public handles before emitting correlated events.

use std::io::Write;

use testlab_schema::{AdapterCommand, AdapterEvent, AdapterEventEnvelope, CommandId};

use crate::AdapterError;
use crate::admission_retry::retry_safe;
use crate::protocol::emit;
use crate::state::AdapterState;

pub(crate) fn dispatch<W: Write>(
    state: &mut AdapterState,
    writer: &mut W,
    command_id: CommandId,
    command: AdapterCommand,
) -> Result<bool, AdapterError> {
    let event = match command {
        AdapterCommand::Flush { producer_id } => {
            let producer = state.producer(&producer_id)?;
            retry_safe(|| producer.flush().wait()).map_err(AdapterError::Client)?;
            AdapterEvent::FlushCompleted { producer_id }
        }
        AdapterCommand::CloseProducer { producer_id } => {
            state.close_producer(&producer_id)?;
            AdapterEvent::ProducerClosed { producer_id }
        }
        AdapterCommand::ShutdownClient { client_id } => {
            state.shutdown_client(&client_id)?;
            AdapterEvent::ClientShutdown { client_id }
        }
        AdapterCommand::Finish => {
            state.finish()?;
            emit(
                writer,
                &AdapterEventEnvelope::new(command_id, AdapterEvent::Finished),
            )?;
            return Ok(true);
        }
        _ => {
            return Err(AdapterError::State(
                "non-lifecycle command reached lifecycle dispatcher".to_owned(),
            ));
        }
    };
    emit(writer, &AdapterEventEnvelope::new(command_id, event))?;
    Ok(false)
}
