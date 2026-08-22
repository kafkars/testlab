//! Subject manifests define exact adapter processes without shell evaluation.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Component, Path};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::SubjectId;

/// Current subject manifest version.
pub const SUBJECT_SCHEMA_VERSION: u16 = 2;

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
    /// Content-addressed packaged artifacts exercised by the adapter.
    #[serde(default)]
    pub artifacts: Vec<SubjectArtifact>,
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
        let mut artifacts = BTreeSet::new();
        for artifact in &self.artifacts {
            artifact.validate()?;
            if !artifacts.insert((artifact.name.as_str(), artifact.version.as_str())) {
                return Err(SubjectError::DuplicateArtifact {
                    name: artifact.name.clone(),
                    version: artifact.version.clone(),
                });
            }
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

/// One exact Cargo package linked through the external subject adapter.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SubjectArtifact {
    /// Cargo package name.
    pub name: String,
    /// Packaged Cargo version.
    pub version: String,
    /// Lowercase SHA-256 of the `.crate` archive.
    pub sha256: String,
}

impl SubjectArtifact {
    fn validate(&self) -> Result<(), SubjectError> {
        if self.name.trim().is_empty() || self.version.trim().is_empty() {
            return Err(SubjectError::ArtifactIdentityEmpty);
        }
        let valid_digest = self.sha256.len() == 64
            && self
                .sha256
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte));
        if !valid_digest {
            return Err(SubjectError::ArtifactDigestInvalid(self.sha256.clone()));
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
    /// A packaged artifact omitted its package identity.
    #[error("subject artifact name and version must not be empty")]
    ArtifactIdentityEmpty,
    /// A packaged artifact digest was not a lowercase SHA-256.
    #[error("invalid subject artifact SHA-256 {0}")]
    ArtifactDigestInvalid(String),
    /// One packaged artifact identity appeared twice.
    #[error("duplicate subject artifact {name}@{version}")]
    DuplicateArtifact {
        /// Duplicate package name.
        name: String,
        /// Duplicate package version.
        version: String,
    },
}
