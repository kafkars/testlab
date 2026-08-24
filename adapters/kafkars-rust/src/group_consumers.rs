//! Classic-group ownership preserves exact public consumers across commands.

use std::collections::BTreeMap;
use std::time::Duration;

use kafkars::{Client, Consumer, ConsumerGroupProtocol, OffsetReset};
use testlab_schema::{ClientId, ConsumerId, GroupProtocol};

use crate::admission_retry::retry_owned_safe;
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
        protocol: GroupProtocol,
    ) -> Result<(), StateError> {
        if self.contains(&consumer_id) {
            return Err(StateError::DuplicateConsumer(consumer_id));
        }
        let builder = client
            .consumer(group_id)
            .subscribe([topic])
            .group_protocol(match protocol {
                GroupProtocol::Classic => ConsumerGroupProtocol::Classic,
                GroupProtocol::Consumer => ConsumerGroupProtocol::Consumer,
            })
            .on_missing_offset(OffsetReset::Earliest)
            .membership_start_timeout(OPERATION_TIMEOUT)
            .close_timeout(OPERATION_TIMEOUT);
        let consumer = retry_owned_safe(builder, |builder| {
            builder
                .build()
                .map_err(kafkars::ConsumerBuildError::into_parts)
        })
        .map_err(|(_, error)| StateError::Client(error))?;
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
        let close = match retry_owned_safe(owner, |owner| {
            let ConsumerOwner {
                client_id,
                consumer,
            } = owner;
            consumer.try_close().map_err(|error| {
                let (consumer, client_error) = error.into_parts();
                (
                    ConsumerOwner {
                        client_id,
                        consumer,
                    },
                    client_error,
                )
            })
        }) {
            Ok(close) => close,
            Err((owner, client_error)) => {
                self.owners.insert(consumer_id.clone(), owner);
                return Err(StateError::Client(client_error));
            }
        };
        close.wait().map_err(StateError::Client)
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
