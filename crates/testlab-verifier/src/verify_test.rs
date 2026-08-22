//! Deterministic verifier tests cover valid admission and delivery truths.

use testlab_schema::{TerminalStatus, VisibilityExpectation};

use super::verify;
use crate::verify_fixture::{adapter, history, observation, rejected_history, scenario};

#[test]
fn explicit_admission_rejection_passes_without_a_terminal() {
    let mut scenario = scenario(
        TerminalStatus::DefinitelyNotSent,
        VisibilityExpectation::Absent,
    );
    scenario.assertions[0].accepted = false;
    scenario.assertions[0].terminal = None;

    let verdict = verify(&scenario, &adapter(), &rejected_history(), &[]);

    assert!(verdict.is_passed());
}

#[test]
fn acknowledged_exact_record_passes() {
    let verdict = verify(
        &scenario(
            TerminalStatus::Acknowledged,
            VisibilityExpectation::ExactlyOnce,
        ),
        &adapter(),
        &history(TerminalStatus::Acknowledged),
        &[observation(0, "value")],
    );

    assert!(verdict.is_passed());
}

#[test]
fn lost_response_preserves_possibly_sent_truth() {
    let verdict = verify(
        &scenario(
            TerminalStatus::PossiblySent,
            VisibilityExpectation::ExactlyOnce,
        ),
        &adapter(),
        &history(TerminalStatus::PossiblySent),
        &[observation(0, "value")],
    );

    assert!(verdict.is_passed());
}
