//! Hosted group shutdown uses only clone-shared control and public event observation.

use std::future::Future;
use std::pin::pin;
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use testlab_schema::{ConsumerId, GroupConsumerShutdownCommand};

use crate::AdapterError;
use crate::state::AdapterState;

const POLL_SLICE: Duration = Duration::from_millis(10);

pub(crate) fn shutdown(
    state: &mut AdapterState,
    command: &GroupConsumerShutdownCommand,
) -> Result<(), AdapterError> {
    let started = Instant::now();
    let deadline = started
        .checked_add(Duration::from_millis(command.timeout_ms))
        .unwrap_or(started);
    let control = state.group_consumer_mut(&command.consumer_id)?.control();
    for _ in 0..command.request_count {
        control.request_shutdown();
    }
    await_termination(state, &command.consumer_id, deadline)?;
    state.remove_shutdown_group_consumer(&command.consumer_id)?;
    Ok(())
}

fn await_termination(
    state: &mut AdapterState,
    consumer_id: &ConsumerId,
    deadline: Instant,
) -> Result<(), AdapterError> {
    loop {
        let observed = {
            let mut next = pin!(state.group_consumer_mut(consumer_id)?.next_event());
            let mut context = Context::from_waker(Waker::noop());
            loop {
                if let Poll::Ready(result) = next.as_mut().poll(&mut context) {
                    break result;
                }
                if Instant::now() >= deadline {
                    return Err(AdapterError::State(format!(
                        "group consumer {consumer_id} event stream did not terminate before deadline"
                    )));
                }
                std::thread::sleep(POLL_SLICE);
            }
        }
        .map_err(AdapterError::Client)?;
        if observed.is_none() {
            return Ok(());
        }
    }
}
