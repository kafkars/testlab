//! Public direct-consumer events are observed without substituting adapter-owned state.

use std::future::Future;
use std::io::Write;
use std::pin::pin;
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use testlab_schema::{
    AdapterEvent, AdapterEventEnvelope, AssignedConsumerEventMethod,
    AssignedConsumerEventObservation, AssignedConsumerEventObservationKind,
    AssignedConsumerFetchFailure, AssignedConsumerFetchFenceObservation,
    AssignedConsumerFetchThrottleFailure, AssignedConsumerPositionFailure,
    AssignedConsumerPositionFenceObservation, CommandId, ObserveAssignedConsumerEventCommand,
};

use crate::kafkars_api::{
    AssignedConsumer, AssignedConsumerEvent, AssignedConsumerFetchFailureKind,
    AssignedConsumerFetchFence, AssignedConsumerFetchThrottleFailureKind,
    AssignedConsumerPositionFence, AssignedConsumerPositionResolutionFailureKind,
};
use crate::protocol::emit;
use crate::{AdapterError, state::AdapterState};

const POLL_SLICE: Duration = Duration::from_millis(10);

pub(crate) fn observe<W: Write>(
    state: &mut AdapterState,
    writer: &mut W,
    command_id: CommandId,
    command: ObserveAssignedConsumerEventCommand,
) -> Result<(), AdapterError> {
    let event = observe_event(
        state.consumer_mut(&command.consumer_id)?,
        command.method,
        command.timeout_ms,
    )?;
    let observation = AssignedConsumerEventObservation {
        operation_id: command.operation_id,
        consumer_id: command.consumer_id,
        method: command.method,
        event: normalize_event(event),
    };
    emit(
        writer,
        &AdapterEventEnvelope::new(
            command_id,
            AdapterEvent::AssignedConsumerEventObserved(observation),
        ),
    )
}

fn observe_event(
    consumer: &mut AssignedConsumer,
    method: AssignedConsumerEventMethod,
    timeout_ms: u64,
) -> Result<AssignedConsumerEvent, AdapterError> {
    let deadline = Instant::now()
        .checked_add(Duration::from_millis(timeout_ms))
        .ok_or_else(|| AdapterError::ConsumerRecord("event deadline overflow".to_owned()))?;
    match method {
        AssignedConsumerEventMethod::NextEvent => observe_waiting(consumer, deadline),
        AssignedConsumerEventMethod::TryTakeEvent => observe_immediate(consumer, deadline),
    }
}

fn observe_waiting(
    consumer: &mut AssignedConsumer,
    deadline: Instant,
) -> Result<AssignedConsumerEvent, AdapterError> {
    let mut next = pin!(consumer.next_event());
    let mut context = Context::from_waker(Waker::noop());
    loop {
        match next.as_mut().poll(&mut context) {
            Poll::Ready(Ok(Some(event))) => return Ok(event),
            Poll::Ready(Ok(None)) => {
                return Err(AdapterError::ConsumerRecord(
                    "assigned-consumer event stream closed".to_owned(),
                ));
            }
            Poll::Ready(Err(error)) => return Err(AdapterError::Client(error)),
            Poll::Pending => {}
        }
        require_before_deadline(deadline)?;
        std::thread::sleep(POLL_SLICE);
    }
}

fn observe_immediate(
    consumer: &mut AssignedConsumer,
    deadline: Instant,
) -> Result<AssignedConsumerEvent, AdapterError> {
    loop {
        match consumer.try_take_event() {
            Ok(Some(event)) => return Ok(event),
            Ok(None) => {}
            Err(error) => return Err(AdapterError::Client(error)),
        }
        require_before_deadline(deadline)?;
        std::thread::sleep(POLL_SLICE);
    }
}

fn require_before_deadline(deadline: Instant) -> Result<(), AdapterError> {
    if Instant::now() >= deadline {
        Err(AdapterError::ConsumerRecord(
            "assigned-consumer event observation timed out".to_owned(),
        ))
    } else {
        Ok(())
    }
}

fn normalize_event(event: AssignedConsumerEvent) -> AssignedConsumerEventObservationKind {
    match event {
        AssignedConsumerEvent::PositionResolutionFailed { fence, kind } => {
            AssignedConsumerEventObservationKind::PositionResolutionFailed {
                fence: normalize_position_fence(&fence),
                failure: normalize_position_failure(kind),
            }
        }
        AssignedConsumerEvent::FetchThrottleFailed { fence, kind } => {
            AssignedConsumerEventObservationKind::FetchThrottleFailed {
                fence: normalize_fetch_fence(&fence),
                failure: match kind {
                    AssignedConsumerFetchThrottleFailureKind::DeadlineOverflow => {
                        AssignedConsumerFetchThrottleFailure::DeadlineOverflow
                    }
                },
            }
        }
        AssignedConsumerEvent::FetchFailed { fence, kind } => {
            AssignedConsumerEventObservationKind::FetchFailed {
                fence: normalize_fetch_fence(&fence),
                failure: normalize_fetch_failure(kind),
            }
        }
    }
}

fn normalize_position_fence(
    fence: &AssignedConsumerPositionFence,
) -> AssignedConsumerPositionFenceObservation {
    AssignedConsumerPositionFenceObservation {
        topic: fence.topic().to_owned(),
        partition: fence.partition(),
        assignment_epoch: fence.assignment_epoch(),
        position_epoch: fence.position_epoch(),
    }
}

fn normalize_fetch_fence(
    fence: &AssignedConsumerFetchFence,
) -> AssignedConsumerFetchFenceObservation {
    AssignedConsumerFetchFenceObservation {
        position: normalize_position_fence(fence.position()),
        fetch_revision: fence.fetch_revision(),
    }
}

fn normalize_position_failure(
    kind: AssignedConsumerPositionResolutionFailureKind,
) -> AssignedConsumerPositionFailure {
    match kind {
        AssignedConsumerPositionResolutionFailureKind::DeadlineElapsed => {
            AssignedConsumerPositionFailure::DeadlineElapsed
        }
        AssignedConsumerPositionResolutionFailureKind::DriverRejected => {
            AssignedConsumerPositionFailure::DriverRejected
        }
        AssignedConsumerPositionResolutionFailureKind::Transport => {
            AssignedConsumerPositionFailure::Transport
        }
        AssignedConsumerPositionResolutionFailureKind::Broker(code) => {
            AssignedConsumerPositionFailure::Broker { code }
        }
        AssignedConsumerPositionResolutionFailureKind::Compatibility => {
            AssignedConsumerPositionFailure::Compatibility
        }
        AssignedConsumerPositionResolutionFailureKind::InvalidResponse => {
            AssignedConsumerPositionFailure::InvalidResponse
        }
        AssignedConsumerPositionResolutionFailureKind::ResponseTooLarge => {
            AssignedConsumerPositionFailure::ResponseTooLarge
        }
        AssignedConsumerPositionResolutionFailureKind::ThrottleDeadlineOverflow => {
            AssignedConsumerPositionFailure::ThrottleDeadlineOverflow
        }
    }
}

fn normalize_fetch_failure(kind: AssignedConsumerFetchFailureKind) -> AssignedConsumerFetchFailure {
    match kind {
        AssignedConsumerFetchFailureKind::DeadlineElapsed => {
            AssignedConsumerFetchFailure::DeadlineElapsed
        }
        AssignedConsumerFetchFailureKind::DriverRejected => {
            AssignedConsumerFetchFailure::DriverRejected
        }
        AssignedConsumerFetchFailureKind::Transport => AssignedConsumerFetchFailure::Transport,
        AssignedConsumerFetchFailureKind::Broker(code) => {
            AssignedConsumerFetchFailure::Broker { code }
        }
        AssignedConsumerFetchFailureKind::Compatibility => {
            AssignedConsumerFetchFailure::Compatibility
        }
        AssignedConsumerFetchFailureKind::InvalidResponse => {
            AssignedConsumerFetchFailure::InvalidResponse
        }
        AssignedConsumerFetchFailureKind::ResponseTooLarge => {
            AssignedConsumerFetchFailure::ResponseTooLarge
        }
    }
}
