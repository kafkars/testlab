//! Adapter state owns public Kafkars handles under protocol identities.

use std::{collections::BTreeMap, time::Duration};

use crate::admission_retry::retry_safe;
use crate::assigned_consumers::AssignedConsumers;
use crate::connection_security::resolve;
use crate::group_consumers::{GroupConsumerRegistration, GroupConsumers};
use crate::kafkars_api::{
    Client, Consumer, ErrorKind, KafkaError, Producer, Security, TransactionalProducer,
};
#[cfg(kafkars_share_candidate)]
use crate::share_consumers::ShareConsumers;
use crate::transactional_producers::{OwnedTransactionalProducer, TransactionalProducers};
use testlab_schema::{AdapterSecurity, ClientId, ConsumerId, ProducerId};

pub(crate) use crate::state_error::StateError;

const DELIVERY_TIMEOUT: Duration = Duration::from_secs(20);

#[derive(Debug, Default)]
pub(crate) struct AdapterState {
    pub(crate) broker_endpoints: Option<Vec<String>>,
    pub(crate) security: Option<Security>,
    pub(crate) clients: BTreeMap<ClientId, Client>,
    producers: BTreeMap<ProducerId, ProducerOwner>,
    pub(crate) consumers: AssignedConsumers,
    group_consumers: GroupConsumers,
    #[cfg(kafkars_share_candidate)]
    pub(crate) share_consumers: ShareConsumers,
    transactional_producers: TransactionalProducers,
    pub(crate) concurrent_group: Option<crate::protocol_concurrent::RunningConcurrentGroup>,
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
    ) -> Result<&mut TransactionalProducer, StateError> {
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
        if self.group_consumers.contains(&consumer_id) || self.share_contains(&consumer_id) {
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
        registration: GroupConsumerRegistration,
    ) -> Result<(), StateError> {
        if self.consumers.contains(&registration.consumer_id)
            || self.share_contains(&registration.consumer_id)
        {
            return Err(StateError::DuplicateConsumer(registration.consumer_id));
        }
        let client = self
            .clients
            .get(&registration.client_id)
            .ok_or_else(|| StateError::MissingClient(registration.client_id.clone()))?;
        self.group_consumers.create(client, registration)
    }

    pub(crate) fn group_consumer_mut(
        &mut self,
        consumer_id: &ConsumerId,
    ) -> Result<&mut Consumer, StateError> {
        self.group_consumers.get_mut(consumer_id)
    }

    pub(crate) fn control_group_consumer(
        &mut self,
        command: &testlab_schema::GroupConsumerControlCommand,
    ) -> Result<(), StateError> {
        self.group_consumers.control(command)
    }

    pub(crate) fn close_group_consumer(
        &mut self,
        consumer_id: &ConsumerId,
    ) -> Result<(), StateError> {
        self.group_consumers.close(consumer_id)
    }

    pub(crate) fn remove_shutdown_group_consumer(
        &mut self,
        consumer_id: &ConsumerId,
    ) -> Result<(), StateError> {
        self.group_consumers.remove_after_shutdown(consumer_id)
    }

    pub(crate) fn close_producer(&mut self, producer_id: &ProducerId) -> Result<(), StateError> {
        let result = {
            let owner = self
                .producers
                .get(producer_id)
                .ok_or_else(|| StateError::MissingProducer(producer_id.clone()))?;
            retry_safe(|| owner.producer.close().wait())
        };
        if let Err(error) = result
            && !is_already_closed(&error)
        {
            return Err(StateError::Client(error));
        }
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
            || self.share_has_owner(client_id)
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
        if self.concurrent_group.is_some() {
            return Err(StateError::UnjoinedConcurrentActors);
        }
        if !self.producers.is_empty() || !self.transactional_producers.is_empty() {
            return Err(StateError::UnclosedProducers);
        }
        if !self.consumers.is_empty() || !self.group_consumers.is_empty() || !self.share_is_empty()
        {
            return Err(StateError::UnclosedConsumers);
        }
        if !self.clients.is_empty() {
            return Err(StateError::UnclosedClients);
        }
        Ok(())
    }
}

pub(crate) fn is_already_closed(error: &KafkaError) -> bool {
    error.kind() == ErrorKind::State && error.to_string() == "producer is already closed"
}
