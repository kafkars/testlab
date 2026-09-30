//! Group seek retries only safe admission and retains one deadline-bound public observer.

use std::future::Future;
use std::pin::pin;
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use crate::admission_retry::retry_until;
use crate::kafkars_api::{
    Consumer, ErrorKind, KafkaError, RetryAdvice, StartPosition, TopicPartition,
};

const POLL_SLICE: Duration = Duration::from_millis(1);

pub(crate) fn seek(
    consumer: &mut Consumer,
    partition: &TopicPartition,
    position: StartPosition,
    deadline: Instant,
) -> Result<(), KafkaError> {
    retry_until(
        deadline,
        || {
            if Instant::now() >= deadline {
                return Err(timeout());
            }
            observe_until(consumer.seek(partition.clone(), position), deadline)
                .unwrap_or_else(|| Err(timeout()))
        },
        |error| {
            error.kind() == ErrorKind::Backpressure
                && error.retry_advice() == RetryAdvice::RetrySafe
        },
    )
}

pub(super) fn observe_until<F: Future>(future: F, deadline: Instant) -> Option<F::Output> {
    let mut future = pin!(future);
    let mut context = Context::from_waker(Waker::noop());
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return None;
        }
        match future.as_mut().poll(&mut context) {
            Poll::Ready(result) => return (Instant::now() < deadline).then_some(result),
            Poll::Pending => std::thread::sleep(POLL_SLICE.min(remaining)),
        }
    }
}

fn timeout() -> KafkaError {
    KafkaError::new(ErrorKind::Timeout, "group seek command deadline elapsed")
}
