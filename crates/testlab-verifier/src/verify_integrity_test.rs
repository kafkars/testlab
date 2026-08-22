//! Verifier integrity tests reject duplication, corruption, and forged digests.

use testlab_schema::{TerminalStatus, VisibilityExpectation};

use super::verify;
use crate::verify_fixture::{adapter, history, observation, record, scenario};

#[test]
fn duplicate_visibility_fails_even_for_uncertain_delivery() {
    let verdict = verify(
        &scenario(
            TerminalStatus::PossiblySent,
            VisibilityExpectation::ZeroOrOne,
        ),
        &adapter(),
        &history(TerminalStatus::PossiblySent),
        &[observation(0, "value"), observation(1, "value")],
    );

    assert!(
        verdict
            .violations
            .iter()
            .any(|value| value.contract_id.as_str() == "PROD-005")
    );
}

#[test]
fn corrupted_visible_record_fails_integrity() {
    let verdict = verify(
        &scenario(
            TerminalStatus::Acknowledged,
            VisibilityExpectation::ExactlyOnce,
        ),
        &adapter(),
        &history(TerminalStatus::Acknowledged),
        &[observation(0, "corrupt")],
    );

    assert!(
        verdict
            .violations
            .iter()
            .any(|value| value.contract_id.as_str() == "PROD-006")
    );
}

#[test]
fn forged_environment_digest_does_not_hide_corruption() {
    let mut observed = observation(0, "corrupt");
    observed.digest = match record("value").digest() {
        Ok(digest) => digest,
        Err(error) => panic!("fixture digest: {error}"),
    };
    let verdict = verify(
        &scenario(
            TerminalStatus::Acknowledged,
            VisibilityExpectation::ExactlyOnce,
        ),
        &adapter(),
        &history(TerminalStatus::Acknowledged),
        &[observed],
    );

    assert!(
        verdict
            .violations
            .iter()
            .any(|value| value.contract_id.as_str() == "PROD-006")
    );
}
