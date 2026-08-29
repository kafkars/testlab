//! Producer cancellation tests pin the curated public outcome mapping.

use testlab_schema::ProducerCancellationOutcome;

use crate::kafkars_api::CancellationOutcome;

#[test]
fn public_cancellation_outcomes_map_without_strengthening() {
    for (public, protocol) in [
        (
            CancellationOutcome::CancelledNotSent,
            ProducerCancellationOutcome::CancelledNotSent,
        ),
        (
            CancellationOutcome::TooLate,
            ProducerCancellationOutcome::TooLate,
        ),
        (
            CancellationOutcome::AlreadyTerminal,
            ProducerCancellationOutcome::AlreadyTerminal,
        ),
    ] {
        assert_eq!(crate::protocol_cancel::map_outcome(public), protocol);
    }
}
