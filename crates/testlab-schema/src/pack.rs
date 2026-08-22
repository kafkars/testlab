//! Scenario packs select reviewable test lanes without hiding their members.

use std::collections::BTreeSet;
use std::path::{Component, Path};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::PackId;

/// One ordered scenario pack.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ScenarioPack {
    /// Exact pack schema version.
    pub schema_version: u16,
    /// Stable pack identity.
    pub id: PackId,
    /// Human-readable lane title.
    pub title: String,
    /// Repository-relative scenario paths.
    pub scenarios: Vec<String>,
}

impl ScenarioPack {
    /// Validates portable paths and unique members.
    pub fn validate(&self) -> Result<(), ScenarioPackError> {
        if self.schema_version != 1 {
            return Err(ScenarioPackError::UnsupportedVersion(self.schema_version));
        }
        if self.title.trim().is_empty() {
            return Err(ScenarioPackError::EmptyTitle);
        }
        if self.scenarios.is_empty() {
            return Err(ScenarioPackError::EmptyPack);
        }
        let mut unique = BTreeSet::new();
        for scenario in &self.scenarios {
            validate_relative_path(scenario)?;
            if !unique.insert(scenario) {
                return Err(ScenarioPackError::DuplicateScenario(scenario.clone()));
            }
        }
        Ok(())
    }
}

fn validate_relative_path(value: &str) -> Result<(), ScenarioPackError> {
    let path = Path::new(value);
    if path.is_absolute() || value.is_empty() {
        return Err(ScenarioPackError::InvalidPath(value.to_owned()));
    }
    let escapes = path.components().any(|component| {
        matches!(
            component,
            Component::ParentDir | Component::RootDir | Component::Prefix(_)
        )
    });
    if escapes {
        return Err(ScenarioPackError::InvalidPath(value.to_owned()));
    }
    Ok(())
}

/// Invalid scenario pack.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum ScenarioPackError {
    /// The pack schema is unknown.
    #[error("unsupported pack schema version {0}")]
    UnsupportedVersion(u16),
    /// The title was empty.
    #[error("pack title must not be empty")]
    EmptyTitle,
    /// No scenario was selected.
    #[error("pack must contain at least one scenario")]
    EmptyPack,
    /// One scenario appeared twice.
    #[error("duplicate scenario path {0}")]
    DuplicateScenario(String),
    /// One path escaped the repository boundary.
    #[error("invalid repository-relative scenario path {0}")]
    InvalidPath(String),
}
