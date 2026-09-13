//! Client state construction applies public policy before starting the shared host.

use testlab_schema::{
    AssignedConsumerConfiguration, ClientConfigurationObservation, ClientId, ProducerConfiguration,
    ProducerConfigurationMethod,
};

use crate::admission_retry::retry_safe;
use crate::kafkars_api::Client;
use crate::state::{AdapterState, StateError};

impl AdapterState {
    pub(crate) fn create_client(
        &mut self,
        client_id: ClientId,
        expected_cluster_id: Option<&str>,
    ) -> Result<ClientConfigurationObservation, StateError> {
        self.create_client_with_configuration(client_id, expected_cluster_id, None, None)
    }

    pub(crate) fn create_configured_client(
        &mut self,
        client_id: ClientId,
        method: ProducerConfigurationMethod,
        configuration: ProducerConfiguration,
    ) -> Result<ClientConfigurationObservation, StateError> {
        self.create_client_with_configuration(client_id, None, Some((method, configuration)), None)
    }

    pub(crate) fn create_assigned_consumer_client(
        &mut self,
        client_id: ClientId,
        configuration: AssignedConsumerConfiguration,
    ) -> Result<ClientConfigurationObservation, StateError> {
        self.create_client_with_configuration(client_id, None, None, Some(configuration))
    }

    fn create_client_with_configuration(
        &mut self,
        client_id: ClientId,
        expected_cluster_id: Option<&str>,
        producer_configuration: Option<(ProducerConfigurationMethod, ProducerConfiguration)>,
        assigned_consumer_configuration: Option<AssignedConsumerConfiguration>,
    ) -> Result<ClientConfigurationObservation, StateError> {
        let endpoints = self
            .broker_endpoints
            .as_ref()
            .ok_or(StateError::HelloRequired)?;
        let security = self.security.clone().ok_or(StateError::HelloRequired)?;
        if self.clients.contains_key(&client_id) {
            return Err(StateError::DuplicateClient(client_id));
        }
        let observes_producer_configuration = producer_configuration.is_some();
        let builder = Client::builder()
            .bootstrap_servers(endpoints.iter().map(String::as_str))
            .client_id(client_id.as_str())
            .security(security);
        let builder = match expected_cluster_id {
            Some(cluster_id) => builder.expected_cluster_id(cluster_id),
            None => builder,
        };
        let builder = match producer_configuration {
            Some((method, configuration)) => {
                crate::producer_configuration::apply(builder, method, configuration)?
            }
            None => builder,
        };
        let builder = match assigned_consumer_configuration {
            Some(configuration) => {
                crate::assigned_consumer_configuration::apply(builder, configuration)?
            }
            None => builder,
        };
        let selected_producer_configuration = observes_producer_configuration
            .then(|| crate::producer_configuration::selected(&builder))
            .transpose()?;
        let client = builder.build().map_err(StateError::Client)?;
        let observation = ClientConfigurationObservation {
            client_id: client_id.clone(),
            observed_client_id: client.client_id().map(str::to_owned),
            observed_bootstrap_servers: client.bootstrap_servers().to_vec(),
            observed_expected_cluster_id: client.expected_cluster_id().map(str::to_owned),
            selected_producer_configuration,
        };
        self.clients.insert(client_id, client);
        Ok(observation)
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
