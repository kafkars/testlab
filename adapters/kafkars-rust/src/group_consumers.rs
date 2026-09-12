//! Classic-group ownership preserves exact public consumers across commands.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use crate::kafkars_api::{
    ClassicGroupAssignor, Client, Consumer, ConsumerBuildError, ConsumerGroupProtocol, OffsetReset,
    RetryAdvice, StartPosition, TopicPartition,
};
use testlab_schema::{
    AssignedStartPosition, ClientId, ConsumerId, GroupClassicAssignor, GroupConsumerConfiguration,
    GroupConsumerControl, GroupConsumerControlCommand, GroupOffsetReset, GroupProtocol,
    GroupReadIsolation,
};

use crate::admission_retry::{retry_owned_safe, retry_owned_until, retry_until};
use crate::group_consumer_configuration::public_classic_group_config;
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

#[derive(Debug)]
pub(crate) struct GroupConsumerRegistration {
    pub(crate) client_id: ClientId,
    pub(crate) consumer_id: ConsumerId,
    pub(crate) group_id: String,
    pub(crate) topics: Vec<String>,
    pub(crate) protocol: GroupProtocol,
    pub(crate) configuration: Option<GroupConsumerConfiguration>,
}

impl GroupConsumers {
    pub(crate) fn create(
        &mut self,
        client: &Client,
        registration: GroupConsumerRegistration,
    ) -> Result<(), StateError> {
        if self.contains(&registration.consumer_id) {
            return Err(StateError::DuplicateConsumer(registration.consumer_id));
        }
        let configuration = registration
            .configuration
            .unwrap_or(GroupConsumerConfiguration {
                offset_reset: GroupOffsetReset::Earliest,
                read_isolation: GroupReadIsolation::ReadUncommitted,
                group_instance_id: None,
                classic_assignor: None,
                classic_session_timeout_ms: None,
                classic_rebalance_timeout_ms: None,
                classic_heartbeat_interval_ms: None,
                classic_heartbeat_attempt_timeout_ms: None,
                classic_rejoin_backoff_ms: None,
                classic_rejoin_attempt_timeout_ms: None,
            });
        let classic_configuration = public_classic_group_config(&configuration);
        let GroupConsumerConfiguration {
            offset_reset,
            read_isolation,
            group_instance_id,
            classic_assignor,
            ..
        } = configuration;
        let builder = client
            .consumer(registration.group_id)
            .subscribe(registration.topics)
            .group_protocol(match registration.protocol {
                GroupProtocol::Classic => ConsumerGroupProtocol::Classic,
                GroupProtocol::Consumer => ConsumerGroupProtocol::Consumer,
            })
            .on_missing_offset(public_offset_reset(offset_reset))
            .read_isolation(public_read_isolation(read_isolation))
            .membership_start_timeout(OPERATION_TIMEOUT)
            .close_timeout(OPERATION_TIMEOUT);
        let builder = match group_instance_id {
            Some(group_instance_id) => builder.group_instance_id(group_instance_id),
            None => builder,
        };
        let builder = match classic_assignor {
            Some(assignor) => builder.classic_group_assignor(public_classic_assignor(assignor)),
            None => builder,
        };
        let builder = match classic_configuration {
            Some(configuration) => builder.classic_group_config(configuration),
            None => builder,
        };
        let consumer = retry_owned_safe(builder, |builder| {
            builder.build().map_err(ConsumerBuildError::into_parts)
        })
        .map_err(|(_, error)| StateError::Client(error))?;
        self.owners.insert(
            registration.consumer_id,
            ConsumerOwner {
                client_id: registration.client_id,
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

    pub(crate) fn control(
        &mut self,
        command: &GroupConsumerControlCommand,
    ) -> Result<(), StateError> {
        let started = std::time::Instant::now();
        let deadline = started
            .checked_add(Duration::from_millis(command.timeout_ms))
            .unwrap_or(started);
        let consumer = self.get_mut(&command.consumer_id)?;
        match &command.control {
            GroupConsumerControl::Pause { partitions } => retry_until(
                deadline,
                || consumer.pause(&public_partitions(partitions)),
                |error| error.retry_advice() == RetryAdvice::RetrySafe,
            ),
            GroupConsumerControl::Resume { partitions } => retry_until(
                deadline,
                || consumer.resume(&public_partitions(partitions)),
                |error| error.retry_advice() == RetryAdvice::RetrySafe,
            ),
            GroupConsumerControl::Seek {
                partition,
                position,
            } => consumer
                .seek(
                    TopicPartition::new(partition.topic.clone(), partition.partition),
                    public_position(*position),
                )
                .wait(),
        }
        .map_err(StateError::Client)
    }

    pub(crate) fn close(&mut self, consumer_id: &ConsumerId) -> Result<(), StateError> {
        let started = Instant::now();
        self.close_until(
            consumer_id,
            started.checked_add(OPERATION_TIMEOUT).unwrap_or(started),
        )
    }

    /// Drops the sole public owner while leaving engine-hosted membership intact.
    pub(crate) fn abandon(&mut self, consumer_id: &ConsumerId) -> Result<(), StateError> {
        let owner = self
            .owners
            .remove(consumer_id)
            .ok_or_else(|| StateError::MissingConsumer(consumer_id.clone()))?;
        drop(owner);
        Ok(())
    }

    pub(crate) fn close_until(
        &mut self,
        consumer_id: &ConsumerId,
        deadline: Instant,
    ) -> Result<(), StateError> {
        let owner = self
            .owners
            .remove(consumer_id)
            .ok_or_else(|| StateError::MissingConsumer(consumer_id.clone()))?;
        let close = match retry_owned_until(
            deadline,
            owner,
            |owner| {
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
            },
            |error| error.retry_advice() == RetryAdvice::RetrySafe,
        ) {
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

fn public_partitions(partitions: &[testlab_schema::TopicPartitionIdentity]) -> Vec<TopicPartition> {
    partitions
        .iter()
        .map(|partition| TopicPartition::new(partition.topic.clone(), partition.partition))
        .collect()
}

const fn public_position(position: AssignedStartPosition) -> StartPosition {
    match position {
        AssignedStartPosition::Beginning => StartPosition::Beginning,
        AssignedStartPosition::End => StartPosition::End,
        AssignedStartPosition::Offset { offset } => StartPosition::Offset(offset),
    }
}

pub(crate) const fn public_offset_reset(offset_reset: GroupOffsetReset) -> OffsetReset {
    match offset_reset {
        GroupOffsetReset::Earliest => OffsetReset::Earliest,
        GroupOffsetReset::Latest => OffsetReset::Latest,
    }
}

pub(crate) const fn public_classic_assignor(
    assignor: GroupClassicAssignor,
) -> ClassicGroupAssignor {
    match assignor {
        GroupClassicAssignor::Range => ClassicGroupAssignor::Range,
        GroupClassicAssignor::CooperativeSticky => ClassicGroupAssignor::CooperativeSticky,
    }
}

pub(crate) const fn public_read_isolation(
    isolation: GroupReadIsolation,
) -> crate::kafkars_api::ReadIsolation {
    match isolation {
        GroupReadIsolation::ReadUncommitted => crate::kafkars_api::ReadIsolation::ReadUncommitted,
        GroupReadIsolation::ReadCommitted => crate::kafkars_api::ReadIsolation::ReadCommitted,
    }
}
