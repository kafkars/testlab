//! Receive services public revocation leases and retains bounded transition evidence.

use std::future::Future;
use std::pin::pin;
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use testlab_schema::ConsumerId;

use crate::AdapterError;
use crate::group_assignment_observe::drain_transitions;
use crate::kafkars_api::{ConsumerBatch, RetryAdvice};
use crate::state::AdapterState;

const POLL_SLICE: Duration = Duration::from_millis(10);

pub(crate) fn receive_batch(
    state: &mut AdapterState,
    consumer_id: &ConsumerId,
    deadline: Instant,
) -> Result<Option<ConsumerBatch>, AdapterError> {
    if let Some(error) = state.group_consumer_mut(consumer_id)?.startup_error() {
        return Err(AdapterError::Client(error));
    }
    loop {
        if drive(state, consumer_id, deadline)? {
            // Dropping this observer starts or cancels no Fetch work. Release
            // its mutable borrow between probes to service revocation events.
            let result = {
                let mut receive = pin!(state.group_consumer_mut(consumer_id)?.recv());
                receive
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
            };
            match result {
                Poll::Ready(Ok(None)) => {
                    if let Some(error) = state.group_consumer_mut(consumer_id)?.startup_error() {
                        return Err(AdapterError::Client(error));
                    }
                    return Ok(None);
                }
                Poll::Ready(Ok(batch)) => return Ok(batch),
                Poll::Ready(Err(error))
                    if error.retry_advice() == RetryAdvice::RetrySafe
                        && Instant::now() < deadline =>
                {
                    // No batch crossed the adapter boundary; reconstruct the
                    // public observation without extending its deadline.
                }
                Poll::Ready(Err(error)) => return Err(AdapterError::Client(error)),
                Poll::Pending => {}
            }
        }
        if Instant::now() >= deadline {
            return Ok(None);
        }
        std::thread::sleep(POLL_SLICE.min(deadline.saturating_duration_since(Instant::now())));
    }
}

pub(crate) fn drive(
    state: &mut AdapterState,
    consumer_id: &ConsumerId,
    deadline: Instant,
) -> Result<bool, AdapterError> {
    let mut pending = std::mem::take(&mut state.pending_group_transitions);
    let result = drain_transitions(
        state,
        std::slice::from_ref(consumer_id),
        deadline,
        &mut pending,
    );
    state.pending_group_transitions = pending;
    result
}
