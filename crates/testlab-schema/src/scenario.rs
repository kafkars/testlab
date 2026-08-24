//! Declarative public scenarios are validated before any subject process starts.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{Capability, OperationAssertion, ScenarioAction, ScenarioId, StepId};

/// Current scenario manifest version.
pub const SCENARIO_SCHEMA_VERSION: u16 = 12;

/// One complete black-box scenario.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Scenario {
    /// Exact scenario schema version.
    pub schema_version: u16,
    /// Stable scenario identity.
    pub id: ScenarioId,
    /// Human-readable title.
    pub title: String,
    /// Reviewable statement of intent.
    pub description: String,
    /// Absolute run timeout measured from scenario execution start.
    pub timeout_ms: u64,
    /// Capabilities required from the adapter.
    #[serde(default)]
    pub requires: BTreeSet<Capability>,
    /// Ordered public and environment actions.
    pub steps: Vec<ScenarioStep>,
    /// Deterministic operation assertions.
    pub assertions: Vec<OperationAssertion>,
}

impl Scenario {
    /// Validates structure, identity ownership, and complete lifecycle.
    pub fn validate(&self) -> Result<(), ScenarioError> {
        crate::scenario_validation::validate(self)
    }
}

/// One named scenario action.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ScenarioStep {
    /// Stable step identity.
    pub id: StepId,
    /// Action payload.
    #[serde(flatten)]
    pub action: ScenarioAction,
}

/// Invalid scenario with all reviewable problems retained.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[error("invalid scenario: {problems:?}")]
pub struct ScenarioError {
    /// Every discovered validation problem.
    pub problems: Vec<String>,
}
