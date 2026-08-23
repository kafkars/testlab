//! Bounded reconstruction of public operations rejected before ownership transfer.

use std::thread;
use std::time::{Duration, Instant};

use kafkars::{KafkaError, RetryAdvice};

const ADMISSION_RETRY_TIMEOUT: Duration = Duration::from_secs(30);
const RETRY_POLL_INTERVAL: Duration = Duration::from_millis(1);

pub(crate) fn retry_safe<T>(
    operation: impl FnMut() -> Result<T, KafkaError>,
) -> Result<T, KafkaError> {
    let started = Instant::now();
    let deadline = started
        .checked_add(ADMISSION_RETRY_TIMEOUT)
        .unwrap_or(started);
    retry_until(deadline, operation, |error| {
        error.retry_advice() == RetryAdvice::RetrySafe
    })
}

pub(crate) fn retry_until<T, E>(
    deadline: Instant,
    mut operation: impl FnMut() -> Result<T, E>,
    retryable: impl Fn(&E) -> bool,
) -> Result<T, E> {
    loop {
        match operation() {
            Ok(value) => return Ok(value),
            Err(error) if retryable(&error) && Instant::now() < deadline => {
                thread::sleep(RETRY_POLL_INTERVAL);
            }
            Err(error) => return Err(error),
        }
    }
}
