//! Scenarios for bounded pre-admission retry without ownership loss.

use std::time::{Duration, Instant};

use super::admission_retry::{retry_owned_until, retry_until, retry_until_with_remaining};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AttemptError {
    Retryable,
    Terminal,
}

#[test]
fn retryable_rejection_reconstructs_the_operation_before_deadline() {
    let mut attempts = 0;
    let result = retry_until(
        Instant::now() + Duration::from_secs(1),
        || {
            attempts += 1;
            if attempts == 1 {
                Err(AttemptError::Retryable)
            } else {
                Ok(7)
            }
        },
        |error| *error == AttemptError::Retryable,
    );

    assert_eq!(result, Ok(7));
    assert_eq!(attempts, 2);
}

#[test]
fn terminal_or_elapsed_rejection_is_returned_without_retry() {
    let mut terminal_attempts = 0;
    let terminal = retry_until(
        Instant::now() + Duration::from_secs(1),
        || {
            terminal_attempts += 1;
            Err::<(), _>(AttemptError::Terminal)
        },
        |error| *error == AttemptError::Retryable,
    );
    let mut elapsed_attempts = 0;
    let elapsed = retry_until(
        Instant::now(),
        || {
            elapsed_attempts += 1;
            Err::<(), _>(AttemptError::Retryable)
        },
        |error| *error == AttemptError::Retryable,
    );

    assert_eq!(terminal, Err(AttemptError::Terminal));
    assert_eq!(terminal_attempts, 1);
    assert_eq!(elapsed, Err(AttemptError::Retryable));
    assert_eq!(elapsed_attempts, 1);
}

#[test]
fn owned_retry_uses_only_the_exact_returned_input() {
    let mut attempts = 0;
    let output = retry_owned_until(
        Instant::now() + Duration::from_secs(1),
        vec![1, 2, 3],
        |mut input| {
            attempts += 1;
            if attempts == 1 {
                input.push(4);
                Err((input, AttemptError::Retryable))
            } else {
                Ok(input)
            }
        },
        |error| *error == AttemptError::Retryable,
    );

    assert_eq!(output, Ok(vec![1, 2, 3, 4]));
    assert_eq!(attempts, 2);
}

#[test]
fn deadline_aware_retry_never_resets_the_remaining_timeout() {
    let timeout = Duration::from_secs(1);
    let deadline = Instant::now() + timeout;
    let mut attempts = Vec::new();
    let result = retry_until_with_remaining(
        deadline,
        |remaining| {
            attempts.push(remaining);
            if attempts.len() == 1 {
                Err(AttemptError::Retryable)
            } else {
                Ok(11)
            }
        },
        |error| *error == AttemptError::Retryable,
    );

    assert_eq!(result, Ok(11));
    assert_eq!(attempts.len(), 2);
    assert!(attempts[0] <= timeout);
    assert!(attempts[1] <= attempts[0]);
}
