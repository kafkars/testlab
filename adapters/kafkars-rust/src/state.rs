//! Adapter state owns public Kafkars handles under protocol identities.

use std::collections::BTreeMap;
use std::time::Duration;

use kafkars::{AssignedConsumer, Client, Producer, Security};
use testlab_schema::{AdapterSecurity, ClientId, ConsumerId, ProducerId};
use thiserror::Error;

use crate::assigned_consumers::AssignedConsumers;
use crate::connection_security::{SecurityError, resolve};

const DELIVERY_TIMEOUT: Duration = Duration::from_secs(20);

#[derive(Debug, Default)]
pub(crate) struct AdapterState {
    broker_endpoints: Option<Vec<String>>,
    security: Option<Security>,
    clients: BTreeMap<ClientId, Client>,
    producers: BTreeMap<ProducerId, ProducerOwner>,
    consumers: AssignedConsumers,
}

#[derive(Debug)]
struct ProducerOwner {
    client_id: ClientId,
    producer: Producer,
}

impl AdapterState {
    pub(crate) fn hello(
        &mut self,
        endpoints: Vec<String>,
        security: AdapterSecurity,
    ) -> Result<(), StateError> {
        if self.broker_endpoints.is_some() {
            return Err(StateError::DuplicateHello);
        }
        let security = resolve(security)?;
        self.broker_endpoints = Some(endpoints);
        self.security = Some(security);
        Ok(())
    }

    pub(crate) fn create_client(&mut self, client_id: ClientId) -> Result<(), StateError> {
        let endpoints = self
            .broker_endpoints
            .as_ref()
            .ok_or(StateError::HelloRequired)?;
        let security = self.security.clone().ok_or(StateError::HelloRequired)?;
        if self.clients.contains_key(&client_id) {
            return Err(StateError::DuplicateClient(client_id));
        }
        let client = Client::builder()
            .bootstrap_servers(endpoints.iter().map(String::as_str))
            .client_id(client_id.as_str())
            .security(security)
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

    pub(crate) fn create_assigned_consumer(
        &mut self,
        client_id: ClientId,
        consumer_id: ConsumerId,
    ) -> Result<(), StateError> {
        let client = self
            .clients
            .get(&client_id)
            .ok_or_else(|| StateError::MissingClient(client_id.clone()))?;
        self.consumers.create(client, client_id, consumer_id)
    }

    pub(crate) fn assign_beginning(
        &mut self,
        consumer_id: &ConsumerId,
        topic: String,
        partition: i32,
    ) -> Result<(), StateError> {
        self.consumers
            .assign_beginning(consumer_id, topic, partition)
    }

    pub(crate) fn consumer_mut(
        &mut self,
        consumer_id: &ConsumerId,
    ) -> Result<&mut AssignedConsumer, StateError> {
        self.consumers.get_mut(consumer_id)
    }

    pub(crate) fn close_assigned_consumer(
        &mut self,
        consumer_id: &ConsumerId,
    ) -> Result<(), StateError> {
        self.consumers.close(consumer_id)
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
        if self.consumers.has_owner(client_id) {
            return Err(StateError::OpenConsumer(client_id.clone()));
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
        if !self.consumers.is_empty() {
            return Err(StateError::UnclosedConsumers);
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
    #[error("consumer {0} already exists")]
    DuplicateConsumer(ConsumerId),
    #[error("consumer {0} does not exist")]
    MissingConsumer(ConsumerId),
    #[error("client {0} still owns an open producer")]
    OpenProducer(ClientId),
    #[error("client {0} still owns an open consumer")]
    OpenConsumer(ClientId),
    #[error("adapter finished with open producers")]
    UnclosedProducers,
    #[error("adapter finished with open consumers")]
    UnclosedConsumers,
    #[error("adapter finished with open clients")]
    UnclosedClients,
    #[error("packaged Kafkars operation failed: {0}")]
    Client(kafkars::KafkaError),
    #[error("adapter connection security failed: {0}")]
    Security(#[from] SecurityError),
}
