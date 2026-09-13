//! Stable offset positions supported by read-only admin scenarios.

use serde::{Deserialize, Serialize};

/// Transactional visibility selected for an Admin offset query.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AdminReadIsolation {
    /// Excludes offsets from unresolved transactions.
    #[default]
    ReadCommitted,
    /// Allows unresolved transactions to influence the selected offset.
    ReadUncommitted,
}

/// One bounded offset position exposed by scenario and adapter schemas.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AdminOffsetPosition {
    /// Selects the earliest available offset.
    Earliest,
    /// Selects the latest available offset.
    Latest,
}

/// One offset selector exposed by a singleton read-only admin scenario.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AdminOffsetSelector {
    /// Selects the earliest available offset.
    Earliest,
    /// Selects the latest available offset.
    Latest,
    /// Selects the record offset carrying the greatest timestamp.
    MaxTimestamp,
    /// Selects the earliest record carrying at least the supplied timestamp.
    Timestamp,
}
