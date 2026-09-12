//! Group checkpoints cover liveness renewal and ordered processed prefixes.

use std::thread;
use std::time::{Duration, Instant};

use crate::AdapterError;
use crate::admission_retry::retry_owned_until;
use crate::kafkars_api::{Checkpoint, Consumer, ConsumerBatch, RetryAdvice};
use testlab_schema::GroupCheckpointMethod;

pub(crate) fn checkpoint(
    consumer: &mut Consumer,
    batch: ConsumerBatch,
    method: GroupCheckpointMethod,
    acknowledgement_delay_ms: u64,
    processed_record_count: Option<usize>,
    deadline: Instant,
) -> Result<Checkpoint, AdapterError> {
    if let Some(count) = processed_record_count {
        if method != GroupCheckpointMethod::Checkpoint {
            return Err(AdapterError::ConsumerRecord(
                "partial checkpoint requires checkpoint_builder".to_owned(),
            ));
        }
        return partial_checkpoint(&batch, count);
    }
    if acknowledgement_delay_ms == 0 {
        return Ok(full_checkpoint(batch, method));
    }
    let delay = Duration::from_millis(acknowledgement_delay_ms);
    pause(delay, deadline)?;
    let acknowledgement = batch.checkpoint_builder().finish();
    retry_owned_until(
        deadline,
        acknowledgement,
        |checkpoint| {
            consumer
                .acknowledge(checkpoint)
                .map_err(|error| error.into_parts())
        },
        |error| error.retry_advice() == RetryAdvice::RetrySafe,
    )
    .map_err(|(_, error)| AdapterError::Client(error))?;
    pause(delay, deadline)?;
    Ok(full_checkpoint(batch, method))
}

fn full_checkpoint(batch: ConsumerBatch, method: GroupCheckpointMethod) -> Checkpoint {
    match method {
        GroupCheckpointMethod::Checkpoint => batch.checkpoint(),
        GroupCheckpointMethod::IntoCheckpoint => batch.into_checkpoint(),
    }
}

fn partial_checkpoint(batch: &ConsumerBatch, count: usize) -> Result<Checkpoint, AdapterError> {
    if count == 0 || count >= batch.len() {
        return Err(AdapterError::ConsumerRecord(
            "processed record count must select a nonempty batch prefix".to_owned(),
        ));
    }
    let mut checkpoint = batch.checkpoint_builder();
    for record in batch.records().take(count) {
        checkpoint
            .mark_processed(&record)
            .map_err(|error| AdapterError::ConsumerRecord(error.to_string()))?;
    }
    Ok(checkpoint.finish())
}

fn pause(delay: Duration, deadline: Instant) -> Result<(), AdapterError> {
    if delay >= deadline.saturating_duration_since(Instant::now()) {
        return Err(AdapterError::ConsumerRecord(
            "processing acknowledgement delay exceeds receive deadline".to_owned(),
        ));
    }
    thread::sleep(delay);
    Ok(())
}
