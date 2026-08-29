//! Client commands own construction, readiness, metrics, and producer-child creation.

use std::io::Write;

use testlab_schema::{AdapterCommand, AdapterEvent, AdapterEventEnvelope, CommandId};

use crate::AdapterError;
use crate::protocol::emit;
use crate::state::AdapterState;

pub(crate) fn dispatch<W: Write>(
    state: &mut AdapterState,
    writer: &mut W,
    command_id: CommandId,
    command: AdapterCommand,
) -> Result<(), AdapterError> {
    let event = match command {
        AdapterCommand::CreateClient { client_id } => {
            state.create_client(client_id.clone())?;
            AdapterEvent::ClientCreated { client_id }
        }
        AdapterCommand::CreateConfiguredClient(action) => {
            state.create_configured_client(action.client_id.clone(), action.configuration)?;
            AdapterEvent::ClientCreated {
                client_id: action.client_id,
            }
        }
        AdapterCommand::AwaitClientReady { client_id } => {
            state.await_client_ready(&client_id)?;
            AdapterEvent::ClientReady { client_id }
        }
        AdapterCommand::ObserveClientMetrics(command) => AdapterEvent::ClientMetricsObserved(
            Box::new(state.observe_client_metrics(command.client_id, command.operation_id)?),
        ),
        AdapterCommand::CreateProducer {
            client_id,
            producer_id,
        } => {
            state.create_producer(client_id, producer_id.clone())?;
            AdapterEvent::ProducerCreated { producer_id }
        }
        _ => {
            return Err(AdapterError::State(
                "non-client command reached client dispatcher".to_owned(),
            ));
        }
    };
    emit(writer, &AdapterEventEnvelope::new(command_id, event))
}
