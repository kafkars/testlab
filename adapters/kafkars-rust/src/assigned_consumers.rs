//! Assigned-consumer ownership stays separate from client and producer state.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use crate::kafkars_api::{
    AssignedConsumer, AssignedConsumerBuildError, Client, RetryAdvice, StartPosition,
    TopicPartition,
};
use testlab_schema::{
    AssignedConsumerControl, AssignedConsumerControlCommand, AssignedPartitionPosition,
    AssignedStartPosition, ClientId, ConsumerId, TopicPartitionIdentity,
};

use crate::admission_retry::{retry_owned_safe, retry_safe, retry_until};
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
        command: &AssignedConsumerControlCommand,
    ) -> Result<(), StateError> {
        let timeout = Duration::from_millis(command.timeout_ms);
        let started = Instant::now();
        let deadline = started.checked_add(timeout).unwrap_or(started);
        let consumer = self.get_mut(&command.consumer_id)?;
        retry_until(
            deadline,
            || apply_control(consumer, &command.control, deadline),
            |error| error.retry_advice() == RetryAdvice::RetrySafe,
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

fn apply_control(
    consumer: &mut AssignedConsumer,
    control: &AssignedConsumerControl,
    deadline: Instant,
) -> Result<(), crate::kafkars_api::KafkaError> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    match control {
        AssignedConsumerControl::Replace { partitions } => {
            consumer.try_replace_assignment(partitions.iter().map(positioned_partition), remaining)
        }
        AssignedConsumerControl::Add { partitions } => {
            consumer.try_add_assignments(partitions.iter().map(positioned_partition), remaining)
        }
        AssignedConsumerControl::Remove { partitions } => consumer.try_remove_assignments(
            partitions
                .iter()
                .map(|entry| TopicPartition::new(entry.topic.clone(), entry.partition)),
        ),
        AssignedConsumerControl::Seek {
            partition,
            position,
        } => consumer.try_seek(
            &TopicPartition::new(partition.topic.clone(), partition.partition),
            start_position(*position),
            remaining,
        ),
        AssignedConsumerControl::Pause { partition } => consumer.try_pause(&TopicPartition::new(
            partition.topic.clone(),
            partition.partition,
        )),
        AssignedConsumerControl::Resume { partition } => consumer.try_resume(
            &TopicPartition::new(partition.topic.clone(), partition.partition),
            remaining,
        ),
    }
}

fn positioned_partition(entry: &AssignedPartitionPosition) -> TopicPartition {
    TopicPartition::new(entry.topic.clone(), entry.partition)
        .start_at(start_position(entry.position))
}

const fn start_position(position: AssignedStartPosition) -> StartPosition {
    match position {
        AssignedStartPosition::Beginning => StartPosition::Beginning,
        AssignedStartPosition::End => StartPosition::End,
        AssignedStartPosition::Offset { offset } => StartPosition::Offset(offset),
    }
}
