//! Immediate event observation retries only safe contention under one fixed deadline.

use std::time::{Duration, Instant};

use crate::assigned_consumer_event_observe::poll_immediate;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Failure {
    SafeContention,
    Terminal,
}

#[test]
fn safe_contention_and_empty_polls_preserve_the_next_exact_event() {
    let mut attempts = 0;
    let result = poll_immediate(
        Instant::now() + Duration::from_secs(1),
        || {
            attempts += 1;
            match attempts {
                1 => Err(Failure::SafeContention),
                2 => Ok(None),
                _ => Ok(Some(17)),
            }
        },
        |error| *error == Failure::SafeContention,
    );
    assert_eq!(result, Some(Ok(17)));
    assert_eq!(attempts, 3);
}

#[test]
fn nonretryable_error_is_returned_without_another_public_attempt() {
    let mut attempts = 0;
    let result = poll_immediate(
        Instant::now() + Duration::from_secs(1),
        || {
            attempts += 1;
            Err::<Option<usize>, _>(Failure::Terminal)
        },
        |error| *error == Failure::SafeContention,
    );
    assert_eq!(result, Some(Err(Failure::Terminal)));
    assert_eq!(attempts, 1);
}

#[test]
fn expired_command_never_attempts_event_extraction() {
    let mut attempts = 0;
    let result = poll_immediate(
        Instant::now(),
        || {
            attempts += 1;
            Ok::<_, Failure>(Some(17))
        },
        |_| false,
    );
    assert_eq!(result, None);
    assert_eq!(attempts, 0);
}

#[test]
fn contention_cannot_reset_the_command_deadline() {
    let deadline = Instant::now() + Duration::from_millis(10);
    let mut attempts = 0;
    let result = poll_immediate(
        deadline,
        || {
            attempts += 1;
            std::thread::sleep(Duration::from_millis(20));
            Err::<Option<usize>, _>(Failure::SafeContention)
        },
        |_| true,
    );
    assert_eq!(result, None);
    assert!(attempts <= 1);
    assert!(Instant::now() >= deadline);
}

#[test]
fn a_late_event_cannot_be_reported_as_success() {
    let deadline = Instant::now() + Duration::from_millis(10);
    let mut attempts = 0;
    let result = poll_immediate(
        deadline,
        || {
            attempts += 1;
            std::thread::sleep(Duration::from_millis(20));
            Ok::<_, Failure>(Some(17))
        },
        |_| false,
    );
    assert_eq!(result, None);
    assert!(attempts <= 1);
}

#[test]
fn public_immediate_method_retains_the_safe_contention_filter() {
    let source = include_str!("assigned_consumer_event_observe.rs");
    assert!(source.contains("poll_immediate(\n        deadline,"));
    assert!(source.contains("|| consumer.try_take_event()"));
    assert!(source.contains("error.kind() == ErrorKind::Backpressure"));
    assert!(source.contains("error.retry_advice() == RetryAdvice::RetrySafe"));
}
