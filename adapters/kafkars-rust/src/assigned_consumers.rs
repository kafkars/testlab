//! Assigned-consumer ownership stays separate from client and producer state.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use crate::kafkars_api::{
    AssignedConsumer, AssignedConsumerBuildError, Client, RetryAdvice, StartPosition,
    TopicPartition,
};
use testlab_schema::{
    AssignedConsumerControlCommand, ClientId, ConsumerId, TopicPartitionIdentity,
};

use crate::admission_retry::{retry_owned_safe, retry_safe, retry_until};
use crate::assigned_consumer_positions::{apply_control, resolve_control};
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

#[derive(Debug)]
pub(crate) struct OwnedAssignedConsumer {
    pub(crate) client_id: ClientId,
    pub(crate) consumer: AssignedConsumer,
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
        let consumer = retry_owned_safe(client.assigned_consumer(), |builder| {
            builder
                .build()
                .map_err(AssignedConsumerBuildError::into_parts)
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

    pub(crate) fn assign_beginning(
        &mut self,
        consumer_id: &ConsumerId,
        topic: &str,
        partition: i32,
    ) -> Result<(), StateError> {
        let started = Instant::now();
        let deadline = started.checked_add(OPERATION_TIMEOUT).unwrap_or(started);
        let consumer = self.get_mut(consumer_id)?;
        retry_until(
            deadline,
            || {
                consumer.try_replace_assignment(
                    [TopicPartition::new(topic, partition).start_at(StartPosition::Beginning)],
                    deadline.saturating_duration_since(Instant::now()),
                )
            },
            |error| error.retry_advice() == RetryAdvice::RetrySafe,
        )
        .map_err(StateError::Client)
    }

    pub(crate) fn assign_beginning_batch(
        &mut self,
        consumer_id: &ConsumerId,
        partitions: &[TopicPartitionIdentity],
        timeout: Duration,
    ) -> Result<(), StateError> {
        let started = Instant::now();
        let deadline = started.checked_add(timeout).unwrap_or(started);
        let consumer = self.get_mut(consumer_id)?;
        retry_until(
            deadline,
            || {
                consumer.try_replace_assignment(
                    partitions.iter().map(|partition| {
                        TopicPartition::new(partition.topic.clone(), partition.partition)
                            .start_at(StartPosition::Beginning)
                    }),
                    deadline.saturating_duration_since(Instant::now()),
                )
            },
            |error| error.retry_advice() == RetryAdvice::RetrySafe,
        )
        .map_err(StateError::Client)
    }

    pub(crate) fn control(
        &mut self,
        client: &Client,
        command: &AssignedConsumerControlCommand,
    ) -> Result<(), StateError> {
        let timeout = Duration::from_millis(command.timeout_ms);
        let started = Instant::now();
        let deadline = started.checked_add(timeout).unwrap_or(started);
        let control =
            resolve_control(client, &command.control, deadline).map_err(StateError::Client)?;
        let consumer = self.get_mut(&command.consumer_id)?;
        retry_until(
            deadline,
            || apply_control(consumer, &control, deadline),
            |error| error.retry_advice() == RetryAdvice::RetrySafe,
        )
        .map_err(StateError::Client)
    }

    pub(crate) fn client_id(&self, consumer_id: &ConsumerId) -> Result<ClientId, StateError> {
        self.owners
            .get(consumer_id)
            .map(|owner| owner.client_id.clone())
            .ok_or_else(|| StateError::MissingConsumer(consumer_id.clone()))
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
        let consumer = self.get_mut(consumer_id)?;
        retry_safe(|| consumer.try_close())
            .map_err(StateError::Client)?
            .wait()
            .map_err(StateError::Client)?;
        self.owners.remove(consumer_id);
        Ok(())
    }

    pub(crate) fn take(
        &mut self,
        consumer_id: &ConsumerId,
    ) -> Result<OwnedAssignedConsumer, StateError> {
        self.owners
            .remove(consumer_id)
            .map(|owner| OwnedAssignedConsumer {
                client_id: owner.client_id,
                consumer: owner.consumer,
            })
            .ok_or_else(|| StateError::MissingConsumer(consumer_id.clone()))
    }

    pub(crate) fn restore(
        &mut self,
        consumer_id: ConsumerId,
        owner: OwnedAssignedConsumer,
    ) -> Result<(), StateError> {
        if self
            .owners
            .insert(
                consumer_id.clone(),
                ConsumerOwner {
                    client_id: owner.client_id,
                    consumer: owner.consumer,
                },
            )
            .is_some()
        {
            return Err(StateError::DuplicateConsumer(consumer_id));
        }
        Ok(())
    }

    pub(crate) fn has_owner(&self, client_id: &ClientId) -> bool {
        self.owners
            .values()
            .any(|owner| &owner.client_id == client_id)
    }

    pub(crate) fn contains(&self, consumer_id: &ConsumerId) -> bool {
        self.owners.contains_key(consumer_id)
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.owners.is_empty()
    }
}
