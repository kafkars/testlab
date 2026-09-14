//! Admin fencing retry tests pin Kafka's transient concurrent-transaction response.

use crate::kafkars_api::RetryAdvice;

#[test]
fn concurrent_transactions_is_retryable_only_for_force_termination() {
    assert!(super::retry_force_termination_parts(
        RetryAdvice::DoNotRetry,
        Some(51),
    ));
    assert!(!super::retry_force_termination_parts(
        RetryAdvice::DoNotRetry,
        Some(42),
    ));
    assert!(super::retry_force_termination_parts(
        RetryAdvice::RetrySafe,
        None,
    ));
}
