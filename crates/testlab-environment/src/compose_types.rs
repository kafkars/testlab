//! Compose lifecycle values carry terminal records and bounded artifacts together.

use std::path::Path;

use testlab_schema::{BrokerObservation, EnvironmentManifest, EnvironmentOperation, RunId};
use thiserror::Error;

use crate::TerminalOutput;

/// Inputs for one isolated Docker Compose environment.
#[derive(Clone, Copy, Debug)]
pub struct ComposeRequest<'a> {
    /// Repository root used as the Compose working directory.
    pub repository_root: &'a Path,
    /// Validated Docker Compose environment manifest.
    pub environment: &'a EnvironmentManifest,
    /// Run identity used to isolate the Compose project and operation IDs.
    pub run_id: &'a RunId,
    /// Wall-clock run start supplied by the harness.
    pub started_unix_ms: u64,
}

/// One retained byte artifact from an environment operation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ComposeArtifact {
    /// Portable evidence filename.
    pub name: String,
    /// Bounded artifact bytes.
    pub bytes: Vec<u8>,
}

/// A bounded lifecycle phase with all terminal evidence retained.
#[derive(Clone, Debug, Default)]
pub struct ComposePhase {
    /// Terminal operations in execution order.
    pub operations: Vec<EnvironmentOperation>,
    /// Operation streams named by their evidence records.
    pub artifacts: Vec<ComposeArtifact>,
    /// First required operation failure, when present.
    pub failure: Option<ComposeFailure>,
}

/// One independent broker snapshot and its terminal environment evidence.
#[derive(Clone, Debug, Default)]
pub struct ComposeObservation {
    /// Observer operation outcome.
    pub phase: ComposePhase,
    /// Exact records visible at the captured broker watermarks.
    pub observations: Vec<BrokerObservation>,
}

impl ComposePhase {
    /// Returns whether every required operation in the phase succeeded.
    pub fn succeeded(&self) -> bool {
        self.failure.is_none()
    }

    pub(super) fn retain(&mut self, output: TerminalOutput) -> bool {
        let succeeded = output.succeeded();
        retain_stream(
            &mut self.artifacts,
            output.operation.stdout_artifact.as_deref(),
            output.stdout,
        );
        retain_stream(
            &mut self.artifacts,
            output.operation.stderr_artifact.as_deref(),
            output.stderr,
        );
        self.operations.push(output.operation);
        succeeded
    }

    pub(super) fn fail(&mut self, code: &'static str, diagnostic: impl Into<String>) {
        if self.failure.is_none() {
            self.failure = Some(ComposeFailure::new(code, diagnostic));
        }
    }
}

/// Stable lifecycle failure suitable for harness invalidity evidence.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[error("{code}: {diagnostic}")]
pub struct ComposeFailure {
    pub(super) code: &'static str,
    pub(super) diagnostic: String,
}

impl ComposeFailure {
    /// Stable machine-readable failure code.
    pub fn code(&self) -> &'static str {
        self.code
    }

    /// Bounded diagnostic supplied by the failed operation.
    pub fn diagnostic(&self) -> &str {
        &self.diagnostic
    }

    pub(super) fn new(code: &'static str, diagnostic: impl Into<String>) -> Self {
        Self {
            code,
            diagnostic: diagnostic.into(),
        }
    }
}

fn retain_stream(artifacts: &mut Vec<ComposeArtifact>, name: Option<&str>, bytes: Vec<u8>) {
    if let Some(name) = name {
        artifacts.push(ComposeArtifact {
            name: name.to_owned(),
            bytes,
        });
    }
}
