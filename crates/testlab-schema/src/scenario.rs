//! Declarative public scenarios are validated before any subject process starts.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::{Capability, OperationAssertion, ScenarioError, ScenarioId, ScenarioStep};

/// Current scenario manifest version.
pub const SCENARIO_SCHEMA_VERSION: u16 = 14;

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
