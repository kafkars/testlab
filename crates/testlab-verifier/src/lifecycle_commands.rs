//! Command-correlated lifecycle verification permits repeated operations on one resource.

use testlab_schema::{AdapterCommand, ProducerId, Scenario, ScenarioAction, Violation};

#[path = "lifecycle_command_matches.rs"]
mod matches;

use crate::index::HistoryIndex;
use crate::support::violation;

pub(crate) fn successful_client_creation_id(
    action: &ScenarioAction,
) -> Option<&testlab_schema::ClientId> {
    match action {
        ScenarioAction::CreateClient(action) if action.expected_error_code.is_none() => {
            Some(&action.client_id)
        }
        ScenarioAction::CreateConfiguredClient(action) => Some(&action.client_id),
        ScenarioAction::CreateAssignedConsumerClient(action) => Some(&action.client_id),
        _ => None,
    }
}

pub(crate) fn verify(scenario: &Scenario, index: &HistoryIndex, violations: &mut Vec<Violation>) {
    let expected_failures = scenario
        .steps
        .iter()
        .filter_map(|step| match &step.action {
            ScenarioAction::CreateClient(action) if action.expected_error_code.is_some() => {
                Some(ExpectedFailure::Client(action))
            }
            ScenarioAction::CreateTransactionalProducer {
                producer_id,
                expected_error_code: Some(_),
                ..
            } => Some(ExpectedFailure::Producer(producer_id)),
            _ => None,
        })
        .collect::<Vec<_>>();
    for (command_sequence, command_id, command) in &index.commands {
        if expected_failures
            .iter()
            .any(|failure| failure.matches(command))
        {
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

enum ExpectedFailure<'a> {
    Client(&'a testlab_schema::CreateClientAction),
    Producer(&'a ProducerId),
}

impl ExpectedFailure<'_> {
    fn matches(&self, command: &AdapterCommand) -> bool {
        matches!(
            (self, command),
            (
                Self::Client(expected),
                AdapterCommand::CreateClient(command)
            ) if expected.client_id == command.client_id
                && expected.expected_cluster_id == command.expected_cluster_id
        ) || matches!(
            (self, command),
            (
                Self::Producer(expected),
                AdapterCommand::CreateTransactionalProducer { producer_id, .. }
            ) if *expected == producer_id
        )
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
    GroupAbandoned,
}

impl<'a> ExpectedLifecycle<'a> {
    #[allow(
        clippy::too_many_lines,
        reason = "exhaustive lifecycle command routing keeps each public command explicit"
    )]
    fn for_command(command: &'a AdapterCommand) -> Option<Self> {
        let expected = match command {
            AdapterCommand::CreateClient(testlab_schema::CreateClientCommand {
                client_id, ..
            })
            | AdapterCommand::CreateConfiguredClient(
                testlab_schema::CreateConfiguredClientAction { client_id, .. },
            )
            | AdapterCommand::CreateAssignedConsumerClient(
                testlab_schema::CreateAssignedConsumerClientAction { client_id, .. },
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
            AdapterCommand::AbandonGroupConsumer(action) => Self::consumer(
                "LIFE-016",
                "group consumer abandonment",
                &action.consumer_id,
                ConsumerEvent::GroupAbandoned,
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
}
