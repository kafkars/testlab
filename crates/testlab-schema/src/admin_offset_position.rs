//! Stable offset positions supported by read-only admin scenarios.

use serde::{Deserialize, Serialize};

/// One bounded offset position exposed by scenario and adapter schemas.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AdminOffsetPosition {
    /// Selects the earliest available offset.
    Earliest,
    /// Selects the latest available offset.
    Latest,
}
