//! Adapter state owns public Kafkars handles under protocol identities.

use std::collections::BTreeMap;
use std::time::Duration;

use kafkars::{Client, Producer};
use testlab_schema::{ClientId, ProducerId};
use thiserror::Error;

const DELIVERY_TIMEOUT: Duration = Duration::from_secs(20);

#[derive(Debug, Default)]
pub(crate) struct AdapterState {
    broker_endpoint: Option<String>,
    clients: BTreeMap<ClientId, Client>,
    producers: BTreeMap<ProducerId, ProducerOwner>,
}

#[derive(Debug)]
struct ProducerOwner {
    client_id: ClientId,
    producer: Producer,
}

impl AdapterState {
    pub(crate) fn hello(&mut self, endpoint: String) -> Result<(), StateError> {
        if self.broker_endpoint.replace(endpoint).is_some() {
            return Err(StateError::DuplicateHello);
        }
        Ok(())
    }

    pub(crate) fn create_client(&mut self, client_id: ClientId) -> Result<(), StateError> {
        let endpoint = self
            .broker_endpoint
            .as_deref()
            .ok_or(StateError::HelloRequired)?;
        if self.clients.contains_key(&client_id) {
            return Err(StateError::DuplicateClient(client_id));
        }
        let client = Client::builder()
            .bootstrap_servers([endpoint])
            .client_id(client_id.as_str())
            .build()
            .map_err(StateError::Client)?;
        self.clients.insert(client_id, client);
        Ok(())
    }

    pub(crate) fn create_producer(
        &mut self,
        client_id: ClientId,
        producer_id: ProducerId,
    ) -> Result<(), StateError> {
        if self.producers.contains_key(&producer_id) {
            return Err(StateError::DuplicateProducer(producer_id));
        }
        let client = self
            .clients
            .get(&client_id)
            .ok_or_else(|| StateError::MissingClient(client_id.clone()))?;
        let producer = client
            .producer()
            .delivery_timeout(DELIVERY_TIMEOUT)
            .build()
            .map_err(StateError::Client)?;
        self.producers.insert(
            producer_id,
            ProducerOwner {
                client_id,
                producer,
            },
        );
        Ok(())
    }

    pub(crate) fn await_client_ready(&self, client_id: &ClientId) -> Result<(), StateError> {
        self.clients
            .get(client_id)
            .ok_or_else(|| StateError::MissingClient(client_id.clone()))?
            .ready()
            .wait()
            .map_err(StateError::Client)
    }

    pub(crate) fn producer(&self, producer_id: &ProducerId) -> Result<&Producer, StateError> {
        self.producers
            .get(producer_id)
            .map(|owner| &owner.producer)
            .ok_or_else(|| StateError::MissingProducer(producer_id.clone()))
    }

    pub(crate) fn close_producer(&mut self, producer_id: &ProducerId) -> Result<(), StateError> {
        let owner = self
            .producers
            .remove(producer_id)
            .ok_or_else(|| StateError::MissingProducer(producer_id.clone()))?;
        owner.producer.close().wait().map_err(StateError::Client)
    }

    pub(crate) fn shutdown_client(&mut self, client_id: &ClientId) -> Result<(), StateError> {
        if self
            .producers
            .values()
            .any(|owner| &owner.client_id == client_id)
        {
            return Err(StateError::OpenProducer(client_id.clone()));
        }
        let client = self
            .clients
            .remove(client_id)
            .ok_or_else(|| StateError::MissingClient(client_id.clone()))?;
        client.shutdown().wait().map_err(StateError::Client)
    }

    pub(crate) fn finish(&self) -> Result<(), StateError> {
        if !self.producers.is_empty() {
            return Err(StateError::UnclosedProducers);
        }
        if !self.clients.is_empty() {
            return Err(StateError::UnclosedClients);
        }
        Ok(())
    }
}

#[derive(Debug, Error)]
pub(crate) enum StateError {
    #[error("hello must be the first command")]
    HelloRequired,
    #[error("hello was received more than once")]
    DuplicateHello,
    #[error("client {0} already exists")]
    DuplicateClient(ClientId),
    #[error("client {0} does not exist")]
    MissingClient(ClientId),
    #[error("producer {0} already exists")]
    DuplicateProducer(ProducerId),
    #[error("producer {0} does not exist")]
    MissingProducer(ProducerId),
    #[error("client {0} still owns an open producer")]
    OpenProducer(ClientId),
    #[error("adapter finished with open producers")]
    UnclosedProducers,
    #[error("adapter finished with open clients")]
    UnclosedClients,
    #[error("packaged Kafkars operation failed: {0}")]
    Client(kafkars::KafkaError),
}
