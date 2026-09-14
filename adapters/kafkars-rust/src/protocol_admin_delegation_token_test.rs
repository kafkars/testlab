//! Delegation-token visibility polling remains singular and deadline bounded.

use super::protocol_admin_delegation_token::{DescriptionDecision, description_decision};

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
