//! Ordinary producer ownership retains its creating client for identity-bound sends.

use testlab_schema::{
    ChildHandleOwnership, ClientId, ProducerHandleConfigurationObservation, ProducerId,
};

use super::{AdapterState, StateError};
use crate::kafkars_api::{Client, Producer};

#[derive(Debug)]
pub(super) struct ProducerOwner {
    pub(super) client_id: ClientId,
    pub(super) producer: Producer,
}

impl AdapterState {
    pub(crate) fn create_producer(
        &mut self,
        client_id: ClientId,
        producer_id: ProducerId,
        ownership: ChildHandleOwnership,
        delivery_timeout_ms: Option<u64>,
    ) -> Result<ProducerHandleConfigurationObservation, StateError> {
        if self.producers.contains_key(&producer_id)
            || self.transactional_producers.contains(&producer_id)
        {
            return Err(StateError::DuplicateProducer(producer_id));
        }
        let client = self
            .clients
            .get(&client_id)
            .ok_or_else(|| StateError::MissingClient(client_id.clone()))?;
        let builder = match ownership {
            ChildHandleOwnership::Shared => client.producer(),
            ChildHandleOwnership::Independent => {
                #[cfg(kafkars_independent_handles_candidate)]
                {
                    client.independent_producer()
                }
                #[cfg(not(kafkars_independent_handles_candidate))]
                {
                    return Err(StateError::IndependentHandlesUnavailable);
                }
            }
        };
        let builder = match delivery_timeout_ms {
            Some(timeout) => builder.delivery_timeout(std::time::Duration::from_millis(timeout)),
            None => builder,
        };
        let selected = builder.selected_delivery_timeout();
        let selected_delivery_timeout_ms = u64::try_from(selected.as_millis()).map_err(|_| {
            StateError::ProducerConfiguration(
                "selected producer delivery timeout exceeded u64 milliseconds".to_owned(),
            )
        })?;
        if std::time::Duration::from_millis(selected_delivery_timeout_ms) != selected {
            return Err(StateError::ProducerConfiguration(
                "selected producer delivery timeout was not whole milliseconds".to_owned(),
            ));
        }
        let observation = ProducerHandleConfigurationObservation {
            producer_id: producer_id.clone(),
            selected_delivery_timeout_ms,
        };
        let producer = builder.build().map_err(StateError::Client)?;
        self.producers.insert(
            producer_id,
            ProducerOwner {
                client_id,
                producer,
            },
        );
        Ok(observation)
    }

    pub(crate) fn producer(&self, producer_id: &ProducerId) -> Result<&Producer, StateError> {
        self.producers
            .get(producer_id)
            .map(|owner| &owner.producer)
            .ok_or_else(|| StateError::MissingProducer(producer_id.clone()))
    }

    pub(crate) fn producer_client(&self, producer_id: &ProducerId) -> Result<Client, StateError> {
        let owner = self
            .producers
            .get(producer_id)
            .ok_or_else(|| StateError::MissingProducer(producer_id.clone()))?;
        self.clients
            .get(&owner.client_id)
            .cloned()
            .ok_or_else(|| StateError::MissingClient(owner.client_id.clone()))
    }
}
