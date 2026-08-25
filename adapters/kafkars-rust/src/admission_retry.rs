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

pub(crate) fn retry_owned_safe<I, O>(
    input: I,
    operation: impl FnMut(I) -> Result<O, (I, KafkaError)>,
) -> Result<O, (I, KafkaError)> {
    let started = Instant::now();
    let deadline = started
        .checked_add(ADMISSION_RETRY_TIMEOUT)
        .unwrap_or(started);
    retry_owned_until(deadline, input, operation, |error| {
        error.retry_advice() == RetryAdvice::RetrySafe
    })
}

pub(crate) fn retry_owned_until<I, O, E>(
    deadline: Instant,
    mut input: I,
    mut operation: impl FnMut(I) -> Result<O, (I, E)>,
    retryable: impl Fn(&E) -> bool,
) -> Result<O, (I, E)> {
    loop {
        match operation(input) {
            Ok(output) => return Ok(output),
            Err((returned, error)) if retryable(&error) && Instant::now() < deadline => {
                input = returned;
                thread::sleep(RETRY_POLL_INTERVAL);
            }
            Err(rejection) => return Err(rejection),
        }
    }
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

pub(crate) fn retry_until_with_remaining<T, E>(
    deadline: Instant,
    mut operation: impl FnMut(Duration) -> Result<T, E>,
    retryable: impl Fn(&E) -> bool,
) -> Result<T, E> {
    retry_until(
        deadline,
        || operation(deadline.saturating_duration_since(Instant::now())),
        retryable,
    )
}
