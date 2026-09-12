//! Creation translation keeps client policy intact across the process boundary.

use crate::runner_protocol::ExpectedEvent;
use testlab_schema::{AdapterCommand, ScenarioAction};

pub(super) fn translate(action: &ScenarioAction) -> Option<(AdapterCommand, ExpectedEvent)> {
    Some(match action {
        ScenarioAction::CreateClient { client_id } => (
            AdapterCommand::CreateClient {
                client_id: client_id.clone(),
            },
            ExpectedEvent::ClientCreated(client_id.clone()),
        ),
        ScenarioAction::CreateConfiguredClient(action) => (
            AdapterCommand::CreateConfiguredClient(action.clone()),
            ExpectedEvent::ClientCreated(action.client_id.clone()),
        ),
        ScenarioAction::CreateAssignedConsumerClient(action) => (
            AdapterCommand::CreateAssignedConsumerClient(action.clone()),
            ExpectedEvent::ClientCreated(action.client_id.clone()),
        ),
        ScenarioAction::AwaitClientReady { client_id } => (
            AdapterCommand::AwaitClientReady {
                client_id: client_id.clone(),
            },
            ExpectedEvent::ClientReady(client_id.clone()),
        ),
        ScenarioAction::CreateProducer {
            client_id,
            producer_id,
            ownership,
        } => (
            AdapterCommand::CreateProducer {
                client_id: client_id.clone(),
                producer_id: producer_id.clone(),
                ownership: *ownership,
            },
            ExpectedEvent::ProducerCreated(producer_id.clone()),
        ),
        _ => return None,
    })
}
