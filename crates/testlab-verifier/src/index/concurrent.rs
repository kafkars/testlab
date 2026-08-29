//! Concurrent history indexing retains command correlation and public actor boundaries.

use testlab_schema::{
    ActorId, AdapterCommand, AdapterEvent, AdapterEventEnvelope, CommandEnvelope, CommandId,
    OperationId,
};

use super::HistoryIndex;

#[derive(Clone, Debug)]
pub(crate) struct IndexedConcurrentStart {
    pub(crate) sequence: u64,
    pub(crate) command_id: CommandId,
    pub(crate) actors: Vec<testlab_schema::ConcurrentActorCommand>,
}

#[derive(Clone, Debug)]
pub(crate) struct IndexedConcurrentJoin {
    pub(crate) sequence: u64,
    pub(crate) command_id: CommandId,
}

#[derive(Clone, Debug)]
pub(crate) struct IndexedConcurrentBoundary {
    pub(crate) sequence: u64,
    pub(crate) command_id: CommandId,
    pub(crate) actor_ids: Vec<ActorId>,
}

#[derive(Clone, Debug)]
pub(crate) struct IndexedConcurrentActorCompletion {
    pub(crate) sequence: u64,
    pub(crate) command_id: CommandId,
    pub(crate) actor_id: ActorId,
    pub(crate) operation_id: OperationId,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ConcurrentPublicEventKind {
    Accepted,
    Rejected,
    Terminal,
    Receive,
}

#[derive(Clone, Debug)]
pub(crate) struct IndexedConcurrentPublicEvent {
    pub(crate) sequence: u64,
    pub(crate) command_id: CommandId,
    pub(crate) operation_id: OperationId,
    pub(crate) kind: ConcurrentPublicEventKind,
}

impl HistoryIndex {
    pub(super) fn record_concurrent_command(
        &mut self,
        envelope: &CommandEnvelope,
        sequence: u64,
    ) -> bool {
        match &envelope.command {
            AdapterCommand::StartConcurrentActors(command) => {
                self.concurrent_starts
                    .entry(command.concurrency_id.clone())
                    .or_default()
                    .push(IndexedConcurrentStart {
                        sequence,
                        command_id: envelope.command_id.clone(),
                        actors: command.actors.clone(),
                    });
                for actor in &command.actors {
                    match actor {
                        testlab_schema::ConcurrentActorCommand::ProducerSend {
                            operation_id,
                            ..
                        } => {
                            self.operations_issued.insert(operation_id.clone());
                        }
                        testlab_schema::ConcurrentActorCommand::AssignedReceive {
                            receive_id,
                            ..
                        } => {
                            self.receives_issued.insert(receive_id.clone());
                        }
                    }
                }
            }
            AdapterCommand::JoinConcurrentActors { concurrency_id, .. } => {
                self.concurrent_joins
                    .entry(concurrency_id.clone())
                    .or_default()
                    .push(IndexedConcurrentJoin {
                        sequence,
                        command_id: envelope.command_id.clone(),
                    });
            }
            _ => return false,
        }
        true
    }

    pub(super) fn record_concurrent_event(
        &mut self,
        envelope: &AdapterEventEnvelope,
        sequence: u64,
    ) {
        match &envelope.event {
            AdapterEvent::ConcurrentActorsStarted {
                concurrency_id,
                actor_ids,
            } => self
                .concurrent_started
                .entry(concurrency_id.clone())
                .or_default()
                .push(boundary(envelope, sequence, actor_ids)),
            AdapterEvent::ConcurrentActorCompleted {
                concurrency_id,
                actor_id,
                operation_id,
            } => self
                .concurrent_actor_completions
                .entry(concurrency_id.clone())
                .or_default()
                .push(IndexedConcurrentActorCompletion {
                    sequence,
                    command_id: envelope.command_id.clone(),
                    actor_id: actor_id.clone(),
                    operation_id: operation_id.clone(),
                }),
            AdapterEvent::ConcurrentActorsCompleted {
                concurrency_id,
                actor_ids,
            } => self
                .concurrent_completed
                .entry(concurrency_id.clone())
                .or_default()
                .push(boundary(envelope, sequence, actor_ids)),
            event => {
                if let Some((operation_id, kind)) = public_event(event) {
                    self.concurrent_public_events
                        .push(IndexedConcurrentPublicEvent {
                            sequence,
                            command_id: envelope.command_id.clone(),
                            operation_id,
                            kind,
                        });
                }
            }
        }
    }
}

fn boundary(
    envelope: &AdapterEventEnvelope,
    sequence: u64,
    actor_ids: &[ActorId],
) -> IndexedConcurrentBoundary {
    IndexedConcurrentBoundary {
        sequence,
        command_id: envelope.command_id.clone(),
        actor_ids: actor_ids.to_vec(),
    }
}

fn public_event(event: &AdapterEvent) -> Option<(OperationId, ConcurrentPublicEventKind)> {
    match event {
        AdapterEvent::OperationAccepted { operation_id } => {
            Some((operation_id.clone(), ConcurrentPublicEventKind::Accepted))
        }
        AdapterEvent::OperationRejected { operation_id, .. } => {
            Some((operation_id.clone(), ConcurrentPublicEventKind::Rejected))
        }
        AdapterEvent::OperationTerminal { operation_id, .. } => {
            Some((operation_id.clone(), ConcurrentPublicEventKind::Terminal))
        }
        AdapterEvent::ReceiveCompleted { receive_id, .. } => {
            Some((receive_id.clone(), ConcurrentPublicEventKind::Receive))
        }
        _ => None,
    }
}
