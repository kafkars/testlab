//! Concurrent verifier support builds adapter commands and stable evidence references.

use testlab_schema::{ConcurrencyId, ConcurrentActor, ConcurrentActorCommand};

use crate::index::{
    HistoryIndex, IndexedConcurrentBoundary, IndexedConcurrentJoin, IndexedConcurrentStart,
};

pub(crate) fn command_actor(actor: &ConcurrentActor) -> ConcurrentActorCommand {
    match actor {
        ConcurrentActor::ProducerSend {
            actor_id,
            producer_id,
            operation_id,
            record,
        } => ConcurrentActorCommand::ProducerSend {
            actor_id: actor_id.clone(),
            producer_id: producer_id.clone(),
            operation_id: operation_id.clone(),
            record: record.clone(),
        },
        ConcurrentActor::AssignedReceive {
            actor_id,
            consumer_id,
            receive_id,
            timeout_ms,
            ..
        } => ConcurrentActorCommand::AssignedReceive {
            actor_id: actor_id.clone(),
            consumer_id: consumer_id.clone(),
            receive_id: receive_id.clone(),
            timeout_ms: *timeout_ms,
        },
    }
}

pub(crate) fn boundary_references(
    starts: Option<&Vec<IndexedConcurrentStart>>,
    joins: Option<&Vec<IndexedConcurrentJoin>>,
    started: Option<&Vec<IndexedConcurrentBoundary>>,
    completed: Option<&Vec<IndexedConcurrentBoundary>>,
) -> Vec<String> {
    starts
        .into_iter()
        .flatten()
        .map(|value| value.sequence)
        .chain(joins.into_iter().flatten().map(|value| value.sequence))
        .chain(started.into_iter().flatten().map(|value| value.sequence))
        .chain(completed.into_iter().flatten().map(|value| value.sequence))
        .map(|sequence| format!("history:{sequence}"))
        .collect()
}

pub(crate) fn concurrent_references(index: &HistoryIndex, id: &ConcurrencyId) -> Vec<String> {
    index
        .concurrent_actor_completions
        .get(id)
        .into_iter()
        .flatten()
        .map(|value| format!("history:{}", value.sequence))
        .collect()
}
