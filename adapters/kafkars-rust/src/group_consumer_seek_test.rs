//! Group seek observation retains one future and cannot outlive its command deadline.

use std::cell::Cell;
use std::future::{Future, poll_fn, ready};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use crate::group_consumer_seek::observe_until;
use crate::kafkars_api::{ErrorKind, KafkaError};

#[test]
fn completed_public_results_are_preserved_without_replacement() {
    let deadline = Instant::now() + Duration::from_secs(1);
    assert_eq!(
        observe_until(ready(Ok::<(), KafkaError>(())), deadline),
        Some(Ok(()))
    );
    let terminal = KafkaError::new(ErrorKind::Transport, "retained public failure");
    assert_eq!(
        observe_until(ready(Err::<(), _>(terminal.clone())), deadline),
        Some(Err(terminal))
    );
}

#[test]
fn pending_seek_keeps_one_observer_until_completion() {
    let polls = Cell::new(0);
    let drops = Cell::new(0);
    assert_eq!(
        observe_until(
            ObservedFuture {
                polls: &polls,
                drops: &drops,
                ready_after: Some(2)
            },
            Instant::now() + Duration::from_secs(1),
        ),
        Some(())
    );
    assert_eq!(polls.get(), 2);
    assert_eq!(drops.get(), 1);
}

#[test]
fn pending_or_unstarted_observation_is_bounded_by_the_original_deadline() {
    for timeout in [Duration::ZERO, Duration::from_millis(5)] {
        let polls = Cell::new(0);
        let drops = Cell::new(0);
        let deadline = Instant::now() + timeout;
        assert_eq!(
            observe_until(
                ObservedFuture {
                    polls: &polls,
                    drops: &drops,
                    ready_after: None
                },
                deadline,
            ),
            None
        );
        if timeout.is_zero() {
            assert_eq!(polls.get(), 0);
        }
        assert_eq!(drops.get(), 1);
        assert!(Instant::now() >= deadline);
    }
}

#[test]
fn completion_observed_after_the_deadline_cannot_be_reported_as_success() {
    let deadline = Instant::now() + Duration::from_millis(5);
    let future = poll_fn(|_| {
        while Instant::now() < deadline {
            std::hint::spin_loop();
        }
        Poll::Ready(())
    });
    assert_eq!(observe_until(future, deadline), None);
}

#[test]
fn group_control_uses_deadline_bound_admission_retry() {
    let control = include_str!("group_consumers.rs");
    let seek = include_str!("group_consumer_seek.rs");
    assert!(control.contains("crate::group_consumer_seek::seek("));
    assert!(seek.contains("retry_until(\n        deadline,"));
    assert!(seek.contains("error.kind() == ErrorKind::Backpressure"));
    assert!(seek.contains("error.retry_advice() == RetryAdvice::RetrySafe"));
}

struct ObservedFuture<'a> {
    polls: &'a Cell<usize>,
    drops: &'a Cell<usize>,
    ready_after: Option<usize>,
}

impl Future for ObservedFuture<'_> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<()> {
        self.polls.set(self.polls.get() + 1);
        if self.ready_after == Some(self.polls.get()) {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

impl Drop for ObservedFuture<'_> {
    fn drop(&mut self) {
        self.drops.set(self.drops.get() + 1);
    }
}
