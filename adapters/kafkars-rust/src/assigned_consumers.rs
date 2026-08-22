//! Assigned-consumer ownership stays separate from client and producer state.

use std::collections::BTreeMap;
use std::time::Duration;

use kafkars::{AssignedConsumer, Client, StartPosition, TopicPartition};
use testlab_schema::{ClientId, ConsumerId};

use crate::state::StateError;

const OPERATION_TIMEOUT: Duration = Duration::from_secs(20);

#[derive(Debug, Default)]
pub(crate) struct AssignedConsumers {
    owners: BTreeMap<ConsumerId, ConsumerOwner>,
}

#[derive(Debug)]
struct ConsumerOwner {
    client_id: ClientId,
    consumer: AssignedConsumer,
}

impl AssignedConsumers {
    pub(crate) fn create(
        &mut self,
        client: &Client,
        client_id: ClientId,
        consumer_id: ConsumerId,
    ) -> Result<(), StateError> {
        if self.owners.contains_key(&consumer_id) {
            return Err(StateError::DuplicateConsumer(consumer_id));
        }
        let consumer = client
            .assigned_consumer()
            .build()
            .map_err(|error| StateError::Client(error.into_parts().1))?;
        self.owners.insert(
            consumer_id,
            ConsumerOwner {
                client_id,
                consumer,
            },
        );
        Ok(())
    }

    pub(crate) fn assign_beginning(
        &mut self,
        consumer_id: &ConsumerId,
        topic: String,
        partition: i32,
    ) -> Result<(), StateError> {
        self.get_mut(consumer_id)?
            .try_replace_assignment(
                [TopicPartition::new(topic, partition).start_at(StartPosition::Beginning)],
                OPERATION_TIMEOUT,
            )
            .map_err(StateError::Client)
    }

    pub(crate) fn get_mut(
        &mut self,
        consumer_id: &ConsumerId,
    ) -> Result<&mut AssignedConsumer, StateError> {
        self.owners
            .get_mut(consumer_id)
            .map(|owner| &mut owner.consumer)
            .ok_or_else(|| StateError::MissingConsumer(consumer_id.clone()))
    }

    pub(crate) fn close(&mut self, consumer_id: &ConsumerId) -> Result<(), StateError> {
        self.get_mut(consumer_id)?
            .try_close()
            .map_err(StateError::Client)?
            .wait()
            .map_err(StateError::Client)?;
        self.owners.remove(consumer_id);
        Ok(())
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
