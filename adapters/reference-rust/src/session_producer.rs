//! Reference producer creation supports only the descriptor's shared-owner path.

use std::io::Write;

use testlab_schema::{
    AdapterEvent, AdapterEventEnvelope, ChildHandleOwnership, ClientId, CommandId,
    ProducerHandleConfigurationObservation, ProducerId,
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
    delivery_timeout_ms: Option<u64>,
) -> Result<(), AdapterError> {
    if delivery_timeout_ms.is_some() {
        return Err(AdapterError::Unsupported(
            "producer_configuration capability required",
        ));
    }
    if ownership.is_independent() {
        return Err(AdapterError::Unsupported(
            "independent_handles capability required",
        ));
    }
    state.create_producer(client_id, producer_id.clone())?;
    emit(
        writer,
        &AdapterEventEnvelope::new(
            command_id,
            AdapterEvent::ProducerCreated(ProducerHandleConfigurationObservation {
                producer_id,
                selected_delivery_timeout_ms: 30_000,
            }),
        ),
    )
}
