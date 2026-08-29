//! Assigned-consumer state operations keep direct assignment details out of core state.

use std::time::Duration;

use crate::kafkars_api::AssignedConsumer;
use testlab_schema::{AssignedConsumerControlCommand, ConsumerId, TopicPartitionIdentity};

use crate::assigned_consumers::OwnedAssignedConsumer;
use crate::state::{AdapterState, StateError};

impl AdapterState {
    pub(crate) fn assign_beginning(
        &mut self,
        consumer_id: &ConsumerId,
        topic: &str,
        partition: i32,
    ) -> Result<(), StateError> {
        self.consumers
            .assign_beginning(consumer_id, topic, partition)
    }

    pub(crate) fn assign_beginning_batch(
        &mut self,
        consumer_id: &ConsumerId,
        partitions: &[TopicPartitionIdentity],
        timeout: Duration,
    ) -> Result<(), StateError> {
        self.consumers
            .assign_beginning_batch(consumer_id, partitions, timeout)
    }

    pub(crate) fn consumer_mut(
        &mut self,
        consumer_id: &ConsumerId,
    ) -> Result<&mut AssignedConsumer, StateError> {
        self.consumers.get_mut(consumer_id)
    }

    pub(crate) fn control_assigned_consumer(
        &mut self,
        command: &AssignedConsumerControlCommand,
    ) -> Result<(), StateError> {
        self.consumers.control(command)
    }

    pub(crate) fn close_assigned_consumer(
        &mut self,
        consumer_id: &ConsumerId,
    ) -> Result<(), StateError> {
        self.consumers.close(consumer_id)
    }

    pub(crate) fn take_assigned_consumer(
        &mut self,
        consumer_id: &ConsumerId,
    ) -> Result<OwnedAssignedConsumer, StateError> {
        self.consumers.take(consumer_id)
    }

    pub(crate) fn restore_assigned_consumer(
        &mut self,
        consumer_id: ConsumerId,
        owner: OwnedAssignedConsumer,
    ) -> Result<(), StateError> {
        self.consumers.restore(consumer_id, owner)
    }
}
