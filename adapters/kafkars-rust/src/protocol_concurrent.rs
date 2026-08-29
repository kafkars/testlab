//! Concurrent commands launch public operations behind one adapter barrier and join in order.

use std::io::Write;
use std::sync::{Arc, Barrier, mpsc};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use testlab_schema::{
    ActorId, AdapterCommand, AdapterEvent, AdapterEventEnvelope, CommandId, ConcurrencyId,
    ConcurrentActorCommand, ConsumerId, OperationId, StartConcurrentActorsCommand,
};

use crate::AdapterError;
use crate::assigned_consumers::OwnedAssignedConsumer;
use crate::protocol::emit;
use crate::protocol_send::SendOutcome;
use crate::state::AdapterState;

#[derive(Debug)]
pub(crate) struct RunningConcurrentGroup {
    concurrency_id: ConcurrencyId,
    actors: Vec<RunningActor>,
}

#[derive(Debug)]
struct RunningActor {
    actor_id: ActorId,
    operation_id: OperationId,
    receiver: mpsc::Receiver<WorkerResult>,
    handle: JoinHandle<()>,
}

#[derive(Debug)]
enum WorkerResult {
    Send(Result<SendOutcome, AdapterError>),
    Receive {
        consumer_id: ConsumerId,
        owner: OwnedAssignedConsumer,
        result: Result<Vec<testlab_schema::ConsumedRecord>, AdapterError>,
    },
}

pub(crate) fn dispatch<W: Write>(
    state: &mut AdapterState,
    writer: &mut W,
    command_id: CommandId,
    command: AdapterCommand,
) -> Result<(), AdapterError> {
    match command {
        AdapterCommand::StartConcurrentActors(command) => start(state, writer, command_id, command),
        AdapterCommand::JoinConcurrentActors {
            concurrency_id,
            timeout_ms,
        } => join(
            state,
            writer,
            command_id,
            &concurrency_id,
            Duration::from_millis(timeout_ms),
        ),
        _ => Err(AdapterError::State(
            "non-concurrent command reached concurrent dispatcher".to_owned(),
        )),
    }
}

fn start<W: Write>(
    state: &mut AdapterState,
    writer: &mut W,
    command_id: CommandId,
    command: StartConcurrentActorsCommand,
) -> Result<(), AdapterError> {
    if !(2..=8).contains(&command.actors.len()) {
        return Err(AdapterError::State(
            "concurrent groups require between 2 and 8 actors".to_owned(),
        ));
    }
    if state.concurrent_group.is_some() {
        return Err(AdapterError::State(
            "a concurrent actor group is already active".to_owned(),
        ));
    }
    let barrier = Arc::new(Barrier::new(command.actors.len() + 1));
    let mut actors = Vec::with_capacity(command.actors.len());
    for actor in command.actors {
        actors.push(spawn_actor(state, Arc::clone(&barrier), actor)?);
    }
    let actor_ids = actors.iter().map(|actor| actor.actor_id.clone()).collect();
    state.concurrent_group = Some(RunningConcurrentGroup {
        concurrency_id: command.concurrency_id.clone(),
        actors,
    });
    barrier.wait();
    emit(
        writer,
        &AdapterEventEnvelope::new(
            command_id,
            AdapterEvent::ConcurrentActorsStarted {
                concurrency_id: command.concurrency_id,
                actor_ids,
            },
        ),
    )
}

fn spawn_actor(
    state: &mut AdapterState,
    barrier: Arc<Barrier>,
    actor: ConcurrentActorCommand,
) -> Result<RunningActor, AdapterError> {
    let actor_id = actor.actor_id().clone();
    let operation_id = actor.operation_id().clone();
    let (sender, receiver) = mpsc::channel();
    let handle = match actor {
        ConcurrentActorCommand::ProducerSend {
            producer_id,
            operation_id,
            record,
            ..
        } => {
            let producer = state.producer(&producer_id)?.clone();
            thread::Builder::new()
                .name(format!("testlab-concurrent-{actor_id}"))
                .spawn(move || {
                    barrier.wait();
                    let result =
                        crate::protocol_send::execute_send(&producer, &operation_id, record);
                    let _ = sender.send(WorkerResult::Send(result));
                })
        }
        ConcurrentActorCommand::AssignedReceive {
            consumer_id,
            timeout_ms,
            ..
        } => {
            let mut owner = state.take_assigned_consumer(&consumer_id)?;
            thread::Builder::new()
                .name(format!("testlab-concurrent-{actor_id}"))
                .spawn(move || {
                    barrier.wait();
                    let result =
                        crate::protocol_consumer::receive_records(&mut owner.consumer, timeout_ms);
                    let _ = sender.send(WorkerResult::Receive {
                        consumer_id,
                        owner,
                        result,
                    });
                })
        }
    }
    .map_err(|error| AdapterError::State(format!("failed to spawn actor {actor_id}: {error}")))?;
    Ok(RunningActor {
        actor_id,
        operation_id,
        receiver,
        handle,
    })
}

fn join<W: Write>(
    state: &mut AdapterState,
    writer: &mut W,
    command_id: CommandId,
    concurrency_id: &ConcurrencyId,
    timeout: Duration,
) -> Result<(), AdapterError> {
    let matches = state
        .concurrent_group
        .as_ref()
        .is_some_and(|group| &group.concurrency_id == concurrency_id);
    if !matches {
        return Err(AdapterError::State(format!(
            "concurrent group {concurrency_id} is not active"
        )));
    }
    let group = state.concurrent_group.take().ok_or_else(|| {
        AdapterError::State(format!("concurrent group {concurrency_id} disappeared"))
    })?;
    let deadline = Instant::now()
        .checked_add(timeout)
        .ok_or_else(|| AdapterError::State("concurrent join deadline overflow".to_owned()))?;
    let actor_ids = group
        .actors
        .iter()
        .map(|actor| actor.actor_id.clone())
        .collect::<Vec<_>>();
    for actor in group.actors {
        join_actor(state, writer, &command_id, concurrency_id, actor, deadline)?;
    }
    emit(
        writer,
        &AdapterEventEnvelope::new(
            command_id,
            AdapterEvent::ConcurrentActorsCompleted {
                concurrency_id: concurrency_id.clone(),
                actor_ids,
            },
        ),
    )
}

fn join_actor<W: Write>(
    state: &mut AdapterState,
    writer: &mut W,
    command_id: &CommandId,
    concurrency_id: &ConcurrencyId,
    actor: RunningActor,
    deadline: Instant,
) -> Result<(), AdapterError> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    let result = actor.receiver.recv_timeout(remaining).map_err(|error| {
        AdapterError::State(format!("actor {} did not join: {error}", actor.actor_id))
    })?;
    actor.handle.join().map_err(|_| {
        AdapterError::State(format!("actor {} panicked during join", actor.actor_id))
    })?;
    match result {
        WorkerResult::Send(result) => crate::protocol_send::emit_send_outcome(
            writer,
            command_id.clone(),
            actor.operation_id.clone(),
            result?,
        )?,
        WorkerResult::Receive {
            consumer_id,
            owner,
            result,
        } => {
            state.restore_assigned_consumer(consumer_id, owner)?;
            crate::protocol_consumer::emit_receive(
                writer,
                command_id.clone(),
                actor.operation_id.clone(),
                result?,
            )?;
        }
    }
    emit(
        writer,
        &AdapterEventEnvelope::new(
            command_id.clone(),
            AdapterEvent::ConcurrentActorCompleted {
                concurrency_id: concurrency_id.clone(),
                actor_id: actor.actor_id,
                operation_id: actor.operation_id,
            },
        ),
    )
}
