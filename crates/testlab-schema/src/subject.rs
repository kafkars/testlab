//! Subject manifests define exact adapter processes without shell evaluation.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Component, Path};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::SubjectId;

/// Current subject manifest version.
pub const SUBJECT_SCHEMA_VERSION: u16 = 1;

/// One packaged client adapter process.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SubjectManifest {
    /// Exact subject schema version.
    pub schema_version: u16,
    /// Stable subject identity.
    pub id: SubjectId,
    /// Human-readable display name.
    pub display_name: String,
    /// Repository-relative or absolute executable path.
    pub command: String,
    /// Exact arguments without shell parsing.
    #[serde(default)]
    pub args: Vec<String>,
    /// Non-secret environment values recorded in evidence.
    #[serde(default)]
    pub environment: BTreeMap<String, String>,
    /// Environment variable names passed from the invoking process.
    #[serde(default)]
    pub pass_environment: Vec<String>,
    /// Optional repository-relative working directory.
    pub working_directory: Option<String>,
}

impl SubjectManifest {
    /// Validates process identity without touching the filesystem.
    pub fn validate(&self) -> Result<(), SubjectError> {
        if self.schema_version != SUBJECT_SCHEMA_VERSION {
            return Err(SubjectError::UnsupportedVersion(self.schema_version));
        }
        if self.display_name.trim().is_empty() {
            return Err(SubjectError::EmptyDisplayName);
        }
        if self.command.trim().is_empty() {
            return Err(SubjectError::EmptyCommand);
        }
        if let Some(directory) = &self.working_directory {
            validate_relative_path(directory)?;
        }
        let mut names = BTreeSet::new();
        for name in &self.pass_environment {
            validate_environment_name(name)?;
            if self.environment.contains_key(name) || !names.insert(name) {
                return Err(SubjectError::DuplicateEnvironment(name.clone()));
            }
        }
        for name in self.environment.keys() {
            validate_environment_name(name)?;
        }
        Ok(())
    }
}

fn validate_relative_path(value: &str) -> Result<(), SubjectError> {
    let path = Path::new(value);
    if path.is_absolute() || value.is_empty() {
        return Err(SubjectError::InvalidWorkingDirectory(value.to_owned()));
    }
    let escapes = path.components().any(|component| {
        matches!(
            component,
            Component::ParentDir | Component::RootDir | Component::Prefix(_)
        )
    });
    if escapes {
        return Err(SubjectError::InvalidWorkingDirectory(value.to_owned()));
    }
    Ok(())
}

fn validate_environment_name(name: &str) -> Result<(), SubjectError> {
    let valid = !name.is_empty()
        && name.chars().all(|character| {
            character.is_ascii_uppercase() || character.is_ascii_digit() || character == '_'
        });
    if valid {
        Ok(())
    } else {
        Err(SubjectError::InvalidEnvironmentName(name.to_owned()))
    }
}

/// Invalid subject process manifest.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum SubjectError {
    /// The subject schema is unknown.
    #[error("unsupported subject schema version {0}")]
    UnsupportedVersion(u16),
    /// The display name was empty.
    #[error("subject display_name must not be empty")]
    EmptyDisplayName,
    /// The executable command was empty.
    #[error("subject command must not be empty")]
    EmptyCommand,
    /// A working directory escaped the repository boundary.
    #[error("invalid repository-relative working directory {0}")]
    InvalidWorkingDirectory(String),
    /// An environment variable name was not portable.
    #[error("invalid environment variable name {0}")]
    InvalidEnvironmentName(String),
    /// One environment variable had two authorities.
    #[error("duplicate environment variable declaration {0}")]
    DuplicateEnvironment(String),
}
