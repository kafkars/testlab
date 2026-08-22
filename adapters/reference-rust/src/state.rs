//! Adapter state owns public-handle identities and lifecycle ordering.

use std::collections::{BTreeMap, BTreeSet};

use testlab_schema::{ClientId, ProducerId};
use thiserror::Error;

#[derive(Debug, Default)]
pub(crate) struct AdapterState {
    broker_endpoints: Option<Vec<String>>,
    clients: BTreeSet<ClientId>,
    producers: BTreeMap<ProducerId, ClientId>,
}

impl AdapterState {
    pub(crate) fn hello(&mut self, endpoints: Vec<String>) -> Result<(), StateError> {
        if self.broker_endpoints.replace(endpoints).is_some() {
            return Err(StateError::DuplicateHello);
        }
        Ok(())
    }

    pub(crate) fn broker_endpoint(&self) -> Result<&str, StateError> {
        self.broker_endpoints
            .as_deref()
            .and_then(|endpoints| endpoints.first())
            .map(String::as_str)
            .ok_or(StateError::HelloRequired)
    }

    pub(crate) fn create_client(&mut self, client_id: ClientId) -> Result<(), StateError> {
        self.require_hello()?;
        if !self.clients.insert(client_id.clone()) {
            return Err(StateError::DuplicateClient(client_id));
        }
        Ok(())
    }

    pub(crate) fn create_producer(
        &mut self,
        client_id: ClientId,
        producer_id: ProducerId,
    ) -> Result<(), StateError> {
        self.require_hello()?;
        if !self.clients.contains(&client_id) {
            return Err(StateError::MissingClient(client_id));
        }
        if self
            .producers
            .insert(producer_id.clone(), client_id)
            .is_some()
        {
            return Err(StateError::DuplicateProducer(producer_id));
        }
        Ok(())
    }

    pub(crate) fn require_client(&self, client_id: &ClientId) -> Result<(), StateError> {
        if self.clients.contains(client_id) {
            Ok(())
        } else {
            Err(StateError::MissingClient(client_id.clone()))
        }
    }

    pub(crate) fn require_producer(&self, producer_id: &ProducerId) -> Result<(), StateError> {
        if self.producers.contains_key(producer_id) {
            Ok(())
        } else {
            Err(StateError::MissingProducer(producer_id.clone()))
        }
    }

    pub(crate) fn close_producer(&mut self, producer_id: &ProducerId) -> Result<(), StateError> {
        if self.producers.remove(producer_id).is_some() {
            Ok(())
        } else {
            Err(StateError::MissingProducer(producer_id.clone()))
        }
    }

    pub(crate) fn shutdown_client(&mut self, client_id: &ClientId) -> Result<(), StateError> {
        let open = self
            .producers
            .values()
            .any(|producer_client| producer_client == client_id);
        if open {
            return Err(StateError::OpenProducer(client_id.clone()));
        }
        if self.clients.remove(client_id) {
            Ok(())
        } else {
            Err(StateError::MissingClient(client_id.clone()))
        }
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

    fn require_hello(&self) -> Result<(), StateError> {
        self.broker_endpoint().map(|_| ())
    }
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
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
}
