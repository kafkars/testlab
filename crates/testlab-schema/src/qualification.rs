//! Qualification manifests pair reviewed environments with scenario packs.

use std::collections::BTreeSet;
use std::path::{Component, Path};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{CellId, QualificationId};

/// Current qualification manifest version.
pub const QUALIFICATION_SCHEMA_VERSION: u16 = 1;

/// One complete and reviewable qualification evidence set.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct QualificationManifest {
    /// Exact qualification schema version.
    pub schema_version: u16,
    /// Stable evidence-set identity.
    pub id: QualificationId,
    /// Human-readable qualification title.
    pub title: String,
    /// Required environment and scenario-pack pairings.
    pub cells: Vec<QualificationCell>,
}

impl QualificationManifest {
    /// Validates exact membership without resolving catalog paths.
    pub fn validate(&self) -> Result<(), QualificationError> {
        if self.schema_version != QUALIFICATION_SCHEMA_VERSION {
            return Err(QualificationError::UnsupportedVersion(self.schema_version));
        }
        if self.title.trim().is_empty() {
            return Err(QualificationError::EmptyTitle);
        }
        if self.cells.is_empty() {
            return Err(QualificationError::EmptySet);
        }
        if !self.cells.iter().any(|cell| cell.gating) {
            return Err(QualificationError::NoGatingCells);
        }
        let mut identities = BTreeSet::new();
        let mut pairings = BTreeSet::new();
        for cell in &self.cells {
            validate_catalog_path(&cell.environment, "clusters")?;
            validate_catalog_path(&cell.pack, "packs")?;
            if !identities.insert(cell.id.as_str()) {
                return Err(QualificationError::DuplicateCellId(cell.id.clone()));
            }
            if !pairings.insert((&cell.environment, &cell.pack)) {
                return Err(QualificationError::DuplicatePairing {
                    environment: cell.environment.clone(),
                    pack: cell.pack.clone(),
                });
            }
        }
        Ok(())
    }
}

/// One required qualification cell.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct QualificationCell {
    /// Stable cell identity used for evidence correlation.
    pub id: CellId,
    /// Repository-relative environment manifest path.
    pub environment: String,
    /// Repository-relative scenario-pack manifest path.
    pub pack: String,
    /// Whether a valid failed verdict blocks qualification.
    pub gating: bool,
}

fn validate_catalog_path(value: &str, root: &str) -> Result<(), QualificationError> {
    let path = Path::new(value);
    let mut components = path.components();
    let starts_at_root = matches!(components.next(), Some(Component::Normal(name)) if name == root);
    let escapes = path.is_absolute()
        || value.is_empty()
        || path.components().any(|component| {
            matches!(
                component,
                Component::ParentDir | Component::RootDir | Component::Prefix(_)
            )
        });
    let extension = path.extension().and_then(std::ffi::OsStr::to_str);
    if starts_at_root && !escapes && extension == Some("toml") {
        Ok(())
    } else {
        Err(QualificationError::CatalogPathInvalid {
            root: root.to_owned(),
            path: value.to_owned(),
        })
    }
}

/// Invalid qualification manifest.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum QualificationError {
    /// The qualification schema is unknown.
    #[error("unsupported qualification schema version {0}")]
    UnsupportedVersion(u16),
    /// The qualification title was empty.
    #[error("qualification title must not be empty")]
    EmptyTitle,
    /// No qualification cells were declared.
    #[error("qualification must contain at least one cell")]
    EmptySet,
    /// No cell could affect the qualification verdict.
    #[error("qualification must contain at least one gating cell")]
    NoGatingCells,
    /// One cell identity appeared twice.
    #[error("duplicate qualification cell id {0}")]
    DuplicateCellId(CellId),
    /// One environment and scenario pack pairing appeared twice.
    #[error("duplicate qualification pairing {environment} with {pack}")]
    DuplicatePairing {
        /// Environment manifest path.
        environment: String,
        /// Scenario-pack manifest path.
        pack: String,
    },
    /// One catalog reference was outside its required root.
    #[error("qualification path must be a {root} TOML manifest: {path}")]
    CatalogPathInvalid {
        /// Required catalog root.
        root: String,
        /// Invalid caller path.
        path: String,
    },
}
