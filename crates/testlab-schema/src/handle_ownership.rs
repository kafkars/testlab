//! Child-handle ownership is explicit at the scenario and adapter boundaries.

use serde::{Deserialize, Serialize};

/// Execution and lifecycle ownership selected for a producer or assigned consumer.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ChildHandleOwnership {
    /// Uses the client-owned execution and lifecycle state.
    #[default]
    Shared,
    /// Starts a private execution and lifecycle owner from the client's configuration.
    Independent,
}

impl ChildHandleOwnership {
    /// Returns whether this handle must own private execution state.
    pub const fn is_independent(self) -> bool {
        matches!(self, Self::Independent)
    }
}

#[cfg(test)]
#[path = "handle_ownership_test.rs"]
mod tests;
