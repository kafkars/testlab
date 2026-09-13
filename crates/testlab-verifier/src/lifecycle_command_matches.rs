//! Exact lifecycle event matching remains separate from command expectation selection.

use testlab_schema::AdapterEvent;

use super::{ClientEvent, ConsumerEvent, ExpectedLifecycle, Identity, ProducerEvent};

impl ExpectedLifecycle<'_> {
    pub(super) fn matches(&self, event: &AdapterEvent) -> bool {
        match (&self.identity, event) {
            (
                Identity::Client(expected, ClientEvent::Created),
                AdapterEvent::ClientCreated(observation),
            ) => *expected == &observation.client_id,
            (
                Identity::Client(expected, ClientEvent::Ready),
                AdapterEvent::ClientReady { client_id },
            )
            | (
                Identity::Client(expected, ClientEvent::Shutdown),
                AdapterEvent::ClientShutdown { client_id },
            ) => *expected == client_id,
            (
                Identity::Producer(expected, ProducerEvent::Created),
                AdapterEvent::ProducerCreated(observation),
            ) => *expected == &observation.producer_id,
            (
                Identity::Producer(expected, ProducerEvent::Flushed),
                AdapterEvent::FlushCompleted { producer_id },
            )
            | (
                Identity::Producer(expected, ProducerEvent::Closed),
                AdapterEvent::ProducerClosed { producer_id },
            )
            | (
                Identity::Producer(expected, ProducerEvent::TransactionalClosed),
                AdapterEvent::TransactionalProducerClosed { producer_id },
            ) => *expected == producer_id,
            (
                Identity::Producer(expected, ProducerEvent::TransactionalCreated),
                AdapterEvent::TransactionalProducerCreated(observation),
            ) => *expected == &observation.producer_id,
            (
                Identity::Consumer(expected, ConsumerEvent::AssignedCreated),
                AdapterEvent::AssignedConsumerCreated { consumer_id },
            )
            | (
                Identity::Consumer(expected, ConsumerEvent::Assigned),
                AdapterEvent::AssignmentCompleted { consumer_id },
            )
            | (
                Identity::Consumer(expected, ConsumerEvent::AssignedClosed),
                AdapterEvent::AssignedConsumerClosed { consumer_id },
            )
            | (
                Identity::Consumer(expected, ConsumerEvent::GroupClosed),
                AdapterEvent::GroupConsumerClosed { consumer_id },
            ) => *expected == consumer_id,
            (
                Identity::Consumer(expected, ConsumerEvent::GroupCreated),
                AdapterEvent::GroupConsumerCreated(observation),
            ) => *expected == &observation.consumer_id,
            (
                Identity::Consumer(expected, ConsumerEvent::GroupAbandoned),
                AdapterEvent::GroupConsumerAbandoned(action),
            ) => *expected == &action.consumer_id,
            (Identity::Finish, AdapterEvent::Finished) => true,
            _ => false,
        }
    }
}
