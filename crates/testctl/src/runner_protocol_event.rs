//! Protocol event outcomes separate intermediate public facts from command completion.

/// Whether one correlated adapter event completes the active command.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum EventDisposition {
    Continue,
    Complete,
}
