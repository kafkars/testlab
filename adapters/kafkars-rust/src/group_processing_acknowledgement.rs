//! Group processing acknowledgement renews liveness before the final checkpoint commit.

use std::thread;
use std::time::{Duration, Instant};

use crate::AdapterError;
use crate::admission_retry::retry_owned_until;
use crate::kafkars_api::{Checkpoint, Consumer, ConsumerBatch, RetryAdvice};

pub(crate) fn checkpoint(
    consumer: &mut Consumer,
    batch: ConsumerBatch,
    delay_ms: u64,
    deadline: Instant,
) -> Result<Checkpoint, AdapterError> {
    if delay_ms == 0 {
        return Ok(batch.checkpoint());
    }
    let delay = Duration::from_millis(delay_ms);
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
    Ok(batch.checkpoint())
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
