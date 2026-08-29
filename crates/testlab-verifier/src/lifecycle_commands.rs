//! Command-correlated lifecycle verification permits repeated operations on one resource.

use testlab_schema::{
    AdapterCommand, AdapterEvent, ProducerId, Scenario, ScenarioAction, Violation,
};

use crate::index::HistoryIndex;
use crate::support::violation;

pub(crate) fn verify(scenario: &Scenario, index: &HistoryIndex, violations: &mut Vec<Violation>) {
    let expected_failures = scenario
        .steps
        .iter()
        .filter_map(|step| match &step.action {
            ScenarioAction::CreateTransactionalProducer {
                producer_id,
                expected_error_code: Some(_),
                ..
            } => Some(producer_id),
            _ => None,
        })
        .collect::<Vec<_>>();
    for (command_sequence, command_id, command) in &index.commands {
        if matches!(
            command,
            AdapterCommand::CreateTransactionalProducer { producer_id, .. }
                if expected_failures.contains(&producer_id)
        ) {
            continue;
        }
        let Some(expected) = ExpectedLifecycle::for_command(command) else {
            continue;
        };
        let correlated = index
            .adapter_events
            .iter()
            .filter(|(_, envelope)| &envelope.command_id == command_id)
            .collect::<Vec<_>>();
        let matching = correlated
            .iter()
            .filter(|(sequence, envelope)| {
                *sequence > *command_sequence && expected.matches(&envelope.event)
            })
            .collect::<Vec<_>>();
        if matching.len() != 1 {
            violations.push(violation(
                expected.contract,
                format!(
                    "command {command_id} expected exactly one correlated {} event, observed {}",
                    expected.operation,
                    matching.len()
                ),
                None,
                correlated
                    .iter()
                    .map(|(sequence, _)| format!("history:{sequence}"))
                    .collect(),
            ));
        }
    }
}

struct ExpectedLifecycle<'a> {
    contract: &'static str,
    operation: &'static str,
    identity: Identity<'a>,
}

enum Identity<'a> {
    Client(&'a testlab_schema::ClientId, ClientEvent),
    Producer(&'a ProducerId, ProducerEvent),
    Consumer(&'a testlab_schema::ConsumerId, ConsumerEvent),
    Finish,
}

#[derive(Clone, Copy)]
enum ClientEvent {
    Created,
    Ready,
    Shutdown,
}

#[derive(Clone, Copy)]
enum ProducerEvent {
    Created,
    Flushed,
    Closed,
    TransactionalCreated,
    TransactionalClosed,
}

#[derive(Clone, Copy)]
enum ConsumerEvent {
    AssignedCreated,
    Assigned,
    AssignedClosed,
    GroupCreated,
    GroupClosed,
}

impl<'a> ExpectedLifecycle<'a> {
    fn for_command(command: &'a AdapterCommand) -> Option<Self> {
        let expected = match command {
            AdapterCommand::CreateClient { client_id }
            | AdapterCommand::CreateConfiguredClient(
                testlab_schema::CreateConfiguredClientAction { client_id, .. },
            ) => Self::client(
                "LIFE-001",
                "client creation",
                client_id,
                ClientEvent::Created,
            ),
            AdapterCommand::AwaitClientReady { client_id } => Self::client(
                "LIFE-007",
                "client readiness",
                client_id,
                ClientEvent::Ready,
            ),
            AdapterCommand::CreateProducer { producer_id, .. } => Self::producer(
                "LIFE-002",
                "producer creation",
                producer_id,
                ProducerEvent::Created,
            ),
            AdapterCommand::Flush { producer_id } => Self::producer(
                "LIFE-003",
                "producer flush",
                producer_id,
                ProducerEvent::Flushed,
            ),
            AdapterCommand::CloseProducer { producer_id } => Self::producer(
                "LIFE-004",
                "producer close",
                producer_id,
                ProducerEvent::Closed,
            ),
            AdapterCommand::ShutdownClient { client_id } => Self::client(
                "LIFE-005",
                "client shutdown",
                client_id,
                ClientEvent::Shutdown,
            ),
            AdapterCommand::CreateAssignedConsumer { consumer_id, .. } => Self::consumer(
                "LIFE-008",
                "assigned consumer creation",
                consumer_id,
                ConsumerEvent::AssignedCreated,
            ),
            AdapterCommand::AssignBeginning { consumer_id, .. }
            | AdapterCommand::AssignBeginningBatch(testlab_schema::AssignBeginningBatchCommand {
                consumer_id,
                ..
            }) => Self::consumer(
                "LIFE-009",
                "direct assignment",
                consumer_id,
                ConsumerEvent::Assigned,
            ),
            AdapterCommand::CloseAssignedConsumer { consumer_id } => Self::consumer(
                "LIFE-010",
                "assigned consumer close",
                consumer_id,
                ConsumerEvent::AssignedClosed,
            ),
            AdapterCommand::CreateGroupConsumer { consumer_id, .. } => Self::consumer(
                "LIFE-011",
                "group consumer creation",
                consumer_id,
                ConsumerEvent::GroupCreated,
            ),
            AdapterCommand::CloseGroupConsumer { consumer_id } => Self::consumer(
                "LIFE-012",
                "group consumer close",
                consumer_id,
                ConsumerEvent::GroupClosed,
            ),
            AdapterCommand::CreateTransactionalProducer { producer_id, .. }
            | AdapterCommand::FenceTransaction {
                replacement_producer_id: producer_id,
                ..
            } => Self::producer(
                "LIFE-013",
                "transactional producer creation",
                producer_id,
                ProducerEvent::TransactionalCreated,
            ),
            AdapterCommand::CloseTransactionalProducer { producer_id } => Self::producer(
                "LIFE-014",
                "transactional producer close",
                producer_id,
                ProducerEvent::TransactionalClosed,
            ),
            AdapterCommand::Finish => Self {
                contract: "LIFE-006",
                operation: "adapter finish",
                identity: Identity::Finish,
            },
            _ => return None,
        };
        Some(expected)
    }

    fn client(
        contract: &'static str,
        operation: &'static str,
        client: &'a testlab_schema::ClientId,
        event: ClientEvent,
    ) -> Self {
        Self {
            contract,
            operation,
            identity: Identity::Client(client, event),
        }
    }

    fn producer(
        contract: &'static str,
        operation: &'static str,
        producer: &'a ProducerId,
        event: ProducerEvent,
    ) -> Self {
        Self {
            contract,
            operation,
            identity: Identity::Producer(producer, event),
        }
    }

    fn consumer(
        contract: &'static str,
        operation: &'static str,
        consumer: &'a testlab_schema::ConsumerId,
        event: ConsumerEvent,
    ) -> Self {
        Self {
            contract,
            operation,
            identity: Identity::Consumer(consumer, event),
        }
    }

    fn matches(&self, event: &AdapterEvent) -> bool {
        match (&self.identity, event) {
            (
                Identity::Client(expected, ClientEvent::Created),
                AdapterEvent::ClientCreated { client_id },
            )
            | (
                Identity::Client(expected, ClientEvent::Ready),
                AdapterEvent::ClientReady { client_id },
            )
            | (
                Identity::Client(expected, ClientEvent::Shutdown),
                AdapterEvent::ClientShutdown { client_id },
            ) => *expected == client_id,
            (
                Identity::Producer(expected, ProducerEvent::Created),
                AdapterEvent::ProducerCreated { producer_id },
            )
            | (
                Identity::Producer(expected, ProducerEvent::Flushed),
                AdapterEvent::FlushCompleted { producer_id },
            )
            | (
                Identity::Producer(expected, ProducerEvent::Closed),
                AdapterEvent::ProducerClosed { producer_id },
            )
            | (
                Identity::Producer(expected, ProducerEvent::TransactionalCreated),
                AdapterEvent::TransactionalProducerCreated { producer_id },
            )
            | (
                Identity::Producer(expected, ProducerEvent::TransactionalClosed),
                AdapterEvent::TransactionalProducerClosed { producer_id },
            ) => *expected == producer_id,
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
                Identity::Consumer(expected, ConsumerEvent::GroupCreated),
                AdapterEvent::GroupConsumerCreated { consumer_id },
            )
            | (
                Identity::Consumer(expected, ConsumerEvent::GroupClosed),
                AdapterEvent::GroupConsumerClosed { consumer_id },
            ) => *expected == consumer_id,
            (Identity::Finish, AdapterEvent::Finished) => true,
            _ => false,
        }
    }
}
