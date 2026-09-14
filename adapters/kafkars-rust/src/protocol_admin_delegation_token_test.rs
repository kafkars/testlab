//! Delegation-token visibility and fresh-token renewal retries remain narrowly bounded.

use crate::kafkars_api::RetryAdvice;

use super::protocol_admin_delegation_token::{
    DescriptionDecision, description_decision, renew_retryable_parts,
};

#[test]
fn an_empty_visible_set_retries_only_before_the_original_deadline() {
    assert_eq!(
        description_decision(0, false),
        Ok(DescriptionDecision::Retry)
    );
    assert!(description_decision(0, true).is_err());
}

#[test]
fn exactly_one_token_is_accepted_and_ambiguous_sets_fail_closed() {
    assert_eq!(
        description_decision(1, false),
        Ok(DescriptionDecision::Accept)
    );
    assert!(description_decision(1, true).is_err());
    assert!(description_decision(2, false).is_err());
}

#[test]
fn only_safe_admission_or_fresh_token_absence_retries() {
    assert!(renew_retryable_parts(
        RetryAdvice::RetrySafe,
        None,
        false,
        false,
    ));
    assert!(renew_retryable_parts(
        RetryAdvice::DoNotRetry,
        Some(62),
        true,
        false,
    ));
    assert!(!renew_retryable_parts(
        RetryAdvice::DoNotRetry,
        Some(62),
        false,
        false,
    ));
    assert!(!renew_retryable_parts(
        RetryAdvice::DoNotRetry,
        Some(62),
        true,
        true,
    ));
    assert!(!renew_retryable_parts(
        RetryAdvice::DoNotRetry,
        Some(61),
        true,
        false,
    ));
}
