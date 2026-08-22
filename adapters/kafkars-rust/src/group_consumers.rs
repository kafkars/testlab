//! Classic-group ownership preserves exact public consumers across commands.

use std::collections::BTreeMap;
use std::time::Duration;

use kafkars::{Client, Consumer, OffsetReset};
use testlab_schema::{ClientId, ConsumerId};

use crate::state::StateError;

const OPERATION_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Default)]
pub(crate) struct GroupConsumers {
    owners: BTreeMap<ConsumerId, ConsumerOwner>,
}

#[derive(Debug)]
struct ConsumerOwner {
    client_id: ClientId,
    consumer: Consumer,
}

impl GroupConsumers {
    pub(crate) fn create(
        &mut self,
        client: &Client,
        client_id: ClientId,
        consumer_id: ConsumerId,
        group_id: String,
        topic: String,
    ) -> Result<(), StateError> {
        if self.contains(&consumer_id) {
            return Err(StateError::DuplicateConsumer(consumer_id));
        }
        let consumer = client
            .consumer(group_id)
            .subscribe([topic])
            .on_missing_offset(OffsetReset::Earliest)
            .membership_start_timeout(OPERATION_TIMEOUT)
            .close_timeout(OPERATION_TIMEOUT)
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

    pub(crate) fn get_mut(
        &mut self,
        consumer_id: &ConsumerId,
    ) -> Result<&mut Consumer, StateError> {
        self.owners
            .get_mut(consumer_id)
            .map(|owner| &mut owner.consumer)
            .ok_or_else(|| StateError::MissingConsumer(consumer_id.clone()))
    }

    pub(crate) fn close(&mut self, consumer_id: &ConsumerId) -> Result<(), StateError> {
        let owner = self
            .owners
            .remove(consumer_id)
            .ok_or_else(|| StateError::MissingConsumer(consumer_id.clone()))?;
        match owner.consumer.try_close() {
            Ok(close) => close.wait().map_err(StateError::Client),
            Err(error) => {
                let (consumer, client_error) = error.into_parts();
                self.owners.insert(
                    consumer_id.clone(),
                    ConsumerOwner {
                        client_id: owner.client_id,
                        consumer,
                    },
                );
                Err(StateError::Client(client_error))
            }
        }
    }

    pub(crate) fn contains(&self, consumer_id: &ConsumerId) -> bool {
        self.owners.contains_key(consumer_id)
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
