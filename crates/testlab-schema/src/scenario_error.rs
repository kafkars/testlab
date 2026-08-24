//! Scenario validation failures retain every independently reviewable problem.

use thiserror::Error;

/// Invalid scenario with all reviewable problems retained.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[error("invalid scenario: {problems:?}")]
pub struct ScenarioError {
    /// Every discovered validation problem.
    pub problems: Vec<String>,
}
