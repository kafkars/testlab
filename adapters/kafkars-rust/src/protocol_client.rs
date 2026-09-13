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
        AdapterCommand::CreateClient(command) => AdapterEvent::ClientCreated(
            state.create_client(command.client_id, command.expected_cluster_id.as_deref())?,
        ),
        AdapterCommand::CreateConfiguredClient(action) => {
            AdapterEvent::ClientCreated(state.create_configured_client(
                action.client_id.clone(),
                action.configuration_method,
                action.configuration,
            )?)
        }
        AdapterCommand::CreateAssignedConsumerClient(action) => AdapterEvent::ClientCreated(
            state.create_assigned_consumer_client(action.client_id, action.configuration)?,
        ),
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
            ownership,
            delivery_timeout_ms,
        } => {
            let observation =
                state.create_producer(client_id, producer_id, ownership, delivery_timeout_ms)?;
            AdapterEvent::ProducerCreated(observation)
        }
        _ => {
            return Err(AdapterError::State(
                "non-client command reached client dispatcher".to_owned(),
            ));
        }
    };
    emit(writer, &AdapterEventEnvelope::new(command_id, event))
}
