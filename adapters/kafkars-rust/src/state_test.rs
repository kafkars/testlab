//! Adapter lifecycle tests distinguish an exact closed handle from other state failures.

use crate::kafkars_api::{ErrorKind, KafkaError};

use crate::state::is_already_closed;

#[test]
fn only_exact_public_already_closed_state_is_idempotent() {
    assert!(is_already_closed(&KafkaError::new(
        ErrorKind::State,
        "producer is already closed",
    )));
    assert!(!is_already_closed(&KafkaError::new(
        ErrorKind::State,
        "producer close observer is stale",
    )));
    assert!(!is_already_closed(&KafkaError::new(
        ErrorKind::Transport,
        "producer is already closed",
    )));
}
