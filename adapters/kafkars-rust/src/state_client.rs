//! Client state construction applies public policy before starting the shared host.

use testlab_schema::{ClientId, ProducerConfiguration};

use crate::admission_retry::retry_safe;
use crate::kafkars_api::Client;
use crate::state::{AdapterState, StateError};

impl AdapterState {
    pub(crate) fn create_client(&mut self, client_id: ClientId) -> Result<(), StateError> {
        self.create_client_with_configuration(client_id, None)
    }

    pub(crate) fn create_configured_client(
        &mut self,
        client_id: ClientId,
        configuration: ProducerConfiguration,
    ) -> Result<(), StateError> {
        self.create_client_with_configuration(client_id, Some(configuration))
    }

    fn create_client_with_configuration(
        &mut self,
        client_id: ClientId,
        configuration: Option<ProducerConfiguration>,
    ) -> Result<(), StateError> {
        let endpoints = self
            .broker_endpoints
            .as_ref()
            .ok_or(StateError::HelloRequired)?;
        let security = self.security.clone().ok_or(StateError::HelloRequired)?;
        if self.clients.contains_key(&client_id) {
            return Err(StateError::DuplicateClient(client_id));
        }
        let builder = Client::builder()
            .bootstrap_servers(endpoints.iter().map(String::as_str))
            .client_id(client_id.as_str())
            .security(security);
        let builder = match configuration {
            Some(configuration) => crate::producer_configuration::apply(builder, configuration)?,
            None => builder,
        };
        let client = builder.build().map_err(StateError::Client)?;
        self.clients.insert(client_id, client);
        Ok(())
    }

    pub(crate) fn await_client_ready(&self, client_id: &ClientId) -> Result<(), StateError> {
        let client = self.client(client_id)?;
        retry_safe(|| client.ready().wait()).map_err(StateError::Client)
    }

    pub(crate) fn client(&self, client_id: &ClientId) -> Result<&Client, StateError> {
        self.clients
            .get(client_id)
            .ok_or_else(|| StateError::MissingClient(client_id.clone()))
    }
}
