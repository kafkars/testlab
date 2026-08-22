//! Transactional producer storage owns public initialization, identity, and close.

use std::collections::BTreeMap;
use std::thread;
use std::time::{Duration, Instant};

use kafkars::{Client, RetryAdvice, TransactionalProducer};
use testlab_schema::{ClientId, ProducerId};

use crate::state::StateError;

#[derive(Debug, Default)]
pub(crate) struct TransactionalProducers {
    owners: BTreeMap<ProducerId, OwnedTransactionalProducer>,
}

#[derive(Debug)]
pub(crate) struct OwnedTransactionalProducer {
    pub(crate) client_id: ClientId,
    pub(crate) producer: TransactionalProducer,
}

impl TransactionalProducers {
    pub(crate) fn create(
        &mut self,
        client: &Client,
        client_id: ClientId,
        producer_id: ProducerId,
        transactional_id: &str,
        transaction_timeout: Duration,
        initialization_timeout: Duration,
    ) -> Result<(), StateError> {
        if self.owners.contains_key(&producer_id) {
            return Err(StateError::DuplicateProducer(producer_id));
        }
        let deadline = Instant::now() + initialization_timeout;
        let producer = loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            let result = client
                .transactional_producer(transactional_id)
                .transaction_timeout(transaction_timeout)
                .deadline_after(remaining)
                .build()
                .wait();
            match result {
                Ok(producer) => break producer,
                Err(error)
                    if error.retry_advice() == RetryAdvice::RetrySafe
                        && Instant::now() < deadline =>
                {
                    thread::sleep(Duration::from_millis(1));
                }
                Err(error) => return Err(StateError::Client(error)),
            }
        };
        self.owners.insert(
            producer_id,
            OwnedTransactionalProducer {
                client_id,
                producer,
            },
        );
        Ok(())
    }

    pub(crate) fn get_mut(
        &mut self,
        producer_id: &ProducerId,
    ) -> Result<&mut TransactionalProducer, StateError> {
        self.owners
            .get_mut(producer_id)
            .map(|owner| &mut owner.producer)
            .ok_or_else(|| StateError::MissingProducer(producer_id.clone()))
    }

    pub(crate) fn close(&mut self, producer_id: &ProducerId) -> Result<(), StateError> {
        let owner = self
            .owners
            .remove(producer_id)
            .ok_or_else(|| StateError::MissingProducer(producer_id.clone()))?;
        owner.producer.close();
        Ok(())
    }

    pub(crate) fn take(
        &mut self,
        producer_id: &ProducerId,
    ) -> Result<OwnedTransactionalProducer, StateError> {
        self.owners
            .remove(producer_id)
            .ok_or_else(|| StateError::MissingProducer(producer_id.clone()))
    }

    pub(crate) fn restore(
        &mut self,
        producer_id: ProducerId,
        owner: OwnedTransactionalProducer,
    ) -> Result<(), StateError> {
        if self.owners.contains_key(&producer_id) {
            return Err(StateError::DuplicateProducer(producer_id));
        }
        self.owners.insert(producer_id, owner);
        Ok(())
    }

    pub(crate) fn contains(&self, producer_id: &ProducerId) -> bool {
        self.owners.contains_key(producer_id)
    }

    pub(crate) fn has_owner(&self, client_id: &ClientId) -> bool {
        self.owners
            .values()
            .any(|owner| &owner.client_id == client_id)
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.owners.is_empty()
    }
}
