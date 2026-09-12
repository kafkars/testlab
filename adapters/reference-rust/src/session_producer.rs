//! Reference producer creation supports only the descriptor's shared-owner path.

use std::io::Write;

use testlab_schema::{
    AdapterEvent, AdapterEventEnvelope, ChildHandleOwnership, ClientId, CommandId, ProducerId,
};

use crate::AdapterError;
use crate::session_output::emit;
use crate::state::AdapterState;

pub(crate) fn dispatch<W: Write>(
    state: &mut AdapterState,
    writer: &mut W,
    command_id: CommandId,
    client_id: ClientId,
    producer_id: ProducerId,
    ownership: ChildHandleOwnership,
) -> Result<(), AdapterError> {
    if ownership.is_independent() {
        return Err(AdapterError::Unsupported(
            "independent_handles capability required",
        ));
    }
    state.create_producer(client_id, producer_id.clone())?;
    emit(
        writer,
        &AdapterEventEnvelope::new(command_id, AdapterEvent::ProducerCreated { producer_id }),
    )
}
