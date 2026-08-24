//! Share close separates retry-safe admission from the accepted public terminal.

use std::thread;
use std::time::{Duration, Instant};

use kafkars::{CloseShareConsumer, KafkaError, RetryAdvice, ShareConsumer};

const POLL_SLICE: Duration = Duration::from_millis(10);

pub(crate) struct ShareCloseOutcome {
    pub(crate) error: Option<KafkaError>,
}

pub(crate) fn admit(
    mut consumer: ShareConsumer,
    timeout: Duration,
) -> Result<CloseShareConsumer, (ShareConsumer, KafkaError)> {
    let started = Instant::now();
    let deadline = started.checked_add(timeout).unwrap_or(started);
    loop {
        match consumer.try_close() {
            Ok(close) => return Ok(close),
            Err(rejection) => {
                let (returned, error) = rejection.into_parts();
                if error.retry_advice() != RetryAdvice::RetrySafe || Instant::now() >= deadline {
                    return Err((returned, error));
                }
                consumer = returned;
                thread::sleep(POLL_SLICE.min(deadline.saturating_duration_since(Instant::now())));
            }
        }
    }
}

pub(crate) fn settle(close: CloseShareConsumer) -> ShareCloseOutcome {
    ShareCloseOutcome {
        error: close.wait().err(),
    }
}
