//! Adapter state owns public Kafkars handles under protocol identities.

use std::collections::BTreeMap;
use std::time::Duration;

use crate::admission_retry::retry_safe;
use crate::assigned_consumers::AssignedConsumers;
use crate::connection_security::resolve;
use crate::group_consumers::GroupConsumers;
use crate::transactional_producers::OwnedTransactionalProducer;
use crate::transactional_producers::TransactionalProducers;
use kafkars::{AssignedConsumer, Client, Consumer, Producer, Security};
use testlab_schema::{AdapterSecurity, ClientId, ConsumerId, GroupProtocol, ProducerId};

pub(crate) use crate::state_error::StateError;

const DELIVERY_TIMEOUT: Duration = Duration::from_secs(20);

#[derive(Debug, Default)]
pub(crate) struct AdapterState {
    broker_endpoints: Option<Vec<String>>,
    security: Option<Security>,
    clients: BTreeMap<ClientId, Client>,
    producers: BTreeMap<ProducerId, ProducerOwner>,
    consumers: AssignedConsumers,
    group_consumers: GroupConsumers,
    transactional_producers: TransactionalProducers,
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
        if self.producers.contains_key(&producer_id)
            || self.transactional_producers.contains(&producer_id)
        {
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
        let client = self
            .clients
            .get(client_id)
            .ok_or_else(|| StateError::MissingClient(client_id.clone()))?;
        retry_safe(|| client.ready().wait()).map_err(StateError::Client)
    }

    pub(crate) fn client(&self, client_id: &ClientId) -> Result<&Client, StateError> {
        self.clients
            .get(client_id)
            .ok_or_else(|| StateError::MissingClient(client_id.clone()))
    }

    pub(crate) fn producer(&self, producer_id: &ProducerId) -> Result<&Producer, StateError> {
        self.producers
            .get(producer_id)
            .map(|owner| &owner.producer)
            .ok_or_else(|| StateError::MissingProducer(producer_id.clone()))
    }

    pub(crate) fn create_transactional_producer(
        &mut self,
        client_id: ClientId,
        producer_id: ProducerId,
        transactional_id: &str,
        transaction_timeout: Duration,
        initialization_timeout: Duration,
    ) -> Result<(), StateError> {
        if self.producers.contains_key(&producer_id) {
            return Err(StateError::DuplicateProducer(producer_id));
        }
        let client = self
            .clients
            .get(&client_id)
            .ok_or_else(|| StateError::MissingClient(client_id.clone()))?;
        self.transactional_producers.create(
            client,
            client_id,
            producer_id,
            transactional_id,
            transaction_timeout,
            initialization_timeout,
        )
    }

    pub(crate) fn transactional_producer_mut(
        &mut self,
        producer_id: &ProducerId,
    ) -> Result<&mut kafkars::TransactionalProducer, StateError> {
        self.transactional_producers.get_mut(producer_id)
    }

    pub(crate) fn close_transactional_producer(
        &mut self,
        producer_id: &ProducerId,
    ) -> Result<(), StateError> {
        self.transactional_producers.close(producer_id)
    }

    pub(crate) fn take_transactional_producer(
        &mut self,
        producer_id: &ProducerId,
    ) -> Result<OwnedTransactionalProducer, StateError> {
        self.transactional_producers.take(producer_id)
    }

    pub(crate) fn restore_transactional_producer(
        &mut self,
        producer_id: ProducerId,
        owner: OwnedTransactionalProducer,
    ) -> Result<(), StateError> {
        self.transactional_producers.restore(producer_id, owner)
    }

    pub(crate) fn create_assigned_consumer(
        &mut self,
        client_id: ClientId,
        consumer_id: ConsumerId,
    ) -> Result<(), StateError> {
        if self.group_consumers.contains(&consumer_id) {
            return Err(StateError::DuplicateConsumer(consumer_id));
        }
        let client = self
            .clients
            .get(&client_id)
            .ok_or_else(|| StateError::MissingClient(client_id.clone()))?;
        self.consumers.create(client, client_id, consumer_id)
    }

    pub(crate) fn create_group_consumer(
        &mut self,
        client_id: ClientId,
        consumer_id: ConsumerId,
        group_id: String,
        topic: String,
        protocol: GroupProtocol,
    ) -> Result<(), StateError> {
        if self.consumers.contains(&consumer_id) {
            return Err(StateError::DuplicateConsumer(consumer_id));
        }
        let client = self
            .clients
            .get(&client_id)
            .ok_or_else(|| StateError::MissingClient(client_id.clone()))?;
        self.group_consumers
            .create(client, client_id, consumer_id, group_id, topic, protocol)
    }

    pub(crate) fn group_consumer_mut(
        &mut self,
        consumer_id: &ConsumerId,
    ) -> Result<&mut Consumer, StateError> {
        self.group_consumers.get_mut(consumer_id)
    }

    pub(crate) fn close_group_consumer(
        &mut self,
        consumer_id: &ConsumerId,
    ) -> Result<(), StateError> {
        self.group_consumers.close(consumer_id)
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
        let result = {
            let owner = self
                .producers
                .get(producer_id)
                .ok_or_else(|| StateError::MissingProducer(producer_id.clone()))?;
            retry_safe(|| owner.producer.close().wait())
        };
        result.map_err(StateError::Client)?;
        self.producers.remove(producer_id);
        Ok(())
    }

    pub(crate) fn shutdown_client(&mut self, client_id: &ClientId) -> Result<(), StateError> {
        if self
            .producers
            .values()
            .any(|owner| &owner.client_id == client_id)
        {
            return Err(StateError::OpenProducer(client_id.clone()));
        }
        if self.consumers.has_owner(client_id)
            || self.group_consumers.has_owner(client_id)
            || self.transactional_producers.has_owner(client_id)
        {
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
        if !self.transactional_producers.is_empty() {
            return Err(StateError::UnclosedProducers);
        }
        if !self.consumers.is_empty() || !self.group_consumers.is_empty() {
            return Err(StateError::UnclosedConsumers);
        }
        if !self.clients.is_empty() {
            return Err(StateError::UnclosedClients);
        }
        Ok(())
    }
}
