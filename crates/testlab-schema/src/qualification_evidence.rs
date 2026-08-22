//! Qualification evidence aggregates sealed scenario verdicts without interpretation.

use std::collections::BTreeSet;
use std::path::{Component, Path};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    CellId, EnvironmentId, PackId, QualificationId, RunId, ScenarioId, SubjectId, VerdictStatus,
};

/// Current qualification evidence manifest version.
pub const QUALIFICATION_EVIDENCE_SCHEMA_VERSION: u16 = 2;

/// One sealed and deterministically aggregated qualification attempt.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct QualificationEvidenceManifest {
    /// Exact qualification evidence schema version.
    pub schema_version: u16,
    /// Unique qualification attempt identity.
    pub run_id: RunId,
    /// Qualification definition under execution.
    pub qualification_id: QualificationId,
    /// Packaged subject under qualification.
    pub subject_id: SubjectId,
    /// Diagnostic start time.
    pub started_unix_ms: u64,
    /// Diagnostic completion time.
    pub completed_unix_ms: u64,
    /// Aggregate status derived from gating cells.
    pub status: VerdictStatus,
    /// Ordered cell evidence matching the qualification manifest.
    pub cells: Vec<QualificationCellEvidence>,
}

impl QualificationEvidenceManifest {
    /// Derives the release-facing status from gating cell evidence.
    pub fn aggregate_status(cells: &[QualificationCellEvidence]) -> VerdictStatus {
        aggregate(
            cells
                .iter()
                .filter(|cell| cell.gating)
                .map(|cell| cell.status),
        )
    }

    /// Validates identities, evidence paths, and deterministic aggregation.
    pub fn validate(&self) -> Result<(), QualificationEvidenceError> {
        if self.schema_version != QUALIFICATION_EVIDENCE_SCHEMA_VERSION {
            return Err(QualificationEvidenceError::UnsupportedVersion(
                self.schema_version,
            ));
        }
        if self.completed_unix_ms < self.started_unix_ms {
            return Err(QualificationEvidenceError::CompletionBeforeStart);
        }
        if self.cells.is_empty() {
            return Err(QualificationEvidenceError::CellsEmpty);
        }
        let mut cell_ids = BTreeSet::new();
        let mut run_ids = BTreeSet::new();
        for cell in &self.cells {
            if !cell_ids.insert(cell.cell_id.as_str()) {
                return Err(QualificationEvidenceError::DuplicateCell(
                    cell.cell_id.clone(),
                ));
            }
            if cell.runs.is_empty() {
                return Err(QualificationEvidenceError::RunsEmpty(cell.cell_id.clone()));
            }
            if cell.attempts == 0 {
                return Err(QualificationEvidenceError::AttemptsEmpty(
                    cell.cell_id.clone(),
                ));
            }
            let mut attempts_seen = BTreeSet::new();
            for run in &cell.runs {
                if run.attempt == 0 || run.attempt > cell.attempts {
                    return Err(QualificationEvidenceError::RunAttemptOutOfRange {
                        cell: cell.cell_id.clone(),
                        attempt: run.attempt,
                        attempts: cell.attempts,
                    });
                }
                attempts_seen.insert(run.attempt);
                validate_path(&run.evidence_path)?;
                let expected_path = format!("cells/{}/{}", cell.cell_id, run.run_id);
                if run.evidence_path != expected_path {
                    return Err(QualificationEvidenceError::EvidencePathMismatch {
                        run: run.run_id.clone(),
                        expected: expected_path,
                        actual: run.evidence_path.clone(),
                    });
                }
                if !run_ids.insert(run.run_id.as_str()) {
                    return Err(QualificationEvidenceError::DuplicateRun(run.run_id.clone()));
                }
            }
            for attempt in 1..=cell.attempts {
                if !attempts_seen.contains(&attempt) {
                    return Err(QualificationEvidenceError::AttemptMissing {
                        cell: cell.cell_id.clone(),
                        attempt,
                    });
                }
            }
            let expected = aggregate_runs(&cell.runs);
            if cell.status != expected {
                return Err(QualificationEvidenceError::CellStatusMismatch {
                    cell: cell.cell_id.clone(),
                    expected,
                    actual: cell.status,
                });
            }
        }
        let expected = Self::aggregate_status(&self.cells);
        if self.status != expected {
            return Err(QualificationEvidenceError::StatusMismatch {
                expected,
                actual: self.status,
            });
        }
        Ok(())
    }
}

/// One environment and scenario-pack cell in a qualification attempt.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct QualificationCellEvidence {
    /// Qualification-local cell identity.
    pub cell_id: CellId,
    /// Exact environment identity used by every cell run.
    pub environment_id: EnvironmentId,
    /// Exact scenario pack identity used by the cell.
    pub pack_id: PackId,
    /// Declared number of independent pack executions.
    pub attempts: u16,
    /// Whether this cell contributes to the aggregate status.
    pub gating: bool,
    /// Aggregate status derived from this cell's scenario runs.
    pub status: VerdictStatus,
    /// Ordered sealed scenario evidence.
    pub runs: Vec<QualificationRunEvidence>,
}

impl QualificationCellEvidence {
    /// Derives one cell status from its ordered scenario run verdicts.
    pub fn aggregate_status(runs: &[QualificationRunEvidence]) -> VerdictStatus {
        aggregate(runs.iter().map(|run| run.status))
    }
}

/// One scenario run referenced by qualification evidence.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct QualificationRunEvidence {
    /// One-based execution ordinal within the qualification cell.
    pub attempt: u16,
    /// Sealed scenario run identity.
    pub run_id: RunId,
    /// Scenario identity from the sealed evidence manifest.
    pub scenario_id: ScenarioId,
    /// Deterministic scenario verdict.
    pub status: VerdictStatus,
    /// Portable path relative to the qualification evidence root.
    pub evidence_path: String,
}

fn aggregate_runs(runs: &[QualificationRunEvidence]) -> VerdictStatus {
    QualificationCellEvidence::aggregate_status(runs)
}

fn aggregate(statuses: impl Iterator<Item = VerdictStatus>) -> VerdictStatus {
    let mut aggregate = VerdictStatus::Passed;
    for status in statuses {
        match status {
            VerdictStatus::Invalid => return VerdictStatus::Invalid,
            VerdictStatus::Failed => aggregate = VerdictStatus::Failed,
            VerdictStatus::Passed => {}
        }
    }
    aggregate
}

fn validate_path(value: &str) -> Result<(), QualificationEvidenceError> {
    let path = Path::new(value);
    let invalid = value.is_empty()
        || path.is_absolute()
        || value.contains('\\')
        || path.components().any(|component| {
            matches!(
                component,
                Component::CurDir
                    | Component::ParentDir
                    | Component::RootDir
                    | Component::Prefix(_)
            )
        });
    if invalid {
        Err(QualificationEvidenceError::EvidencePathInvalid(
            value.to_owned(),
        ))
    } else {
        Ok(())
    }
}

/// Invalid qualification aggregate evidence.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum QualificationEvidenceError {
    /// The schema version is unknown.
    #[error("unsupported qualification evidence schema version {0}")]
    UnsupportedVersion(u16),
    /// Completion predates the attempt start.
    #[error("qualification completion predates its start")]
    CompletionBeforeStart,
    /// No cell evidence was present.
    #[error("qualification evidence must contain cells")]
    CellsEmpty,
    /// A cell identity appeared more than once.
    #[error("duplicate qualification evidence cell {0}")]
    DuplicateCell(CellId),
    /// A cell contained no scenario runs.
    #[error("qualification evidence cell {0} contains no runs")]
    RunsEmpty(CellId),
    /// One cell declared no execution attempts.
    #[error("qualification evidence cell {0} declares no attempts")]
    AttemptsEmpty(CellId),
    /// One run referenced an attempt outside the cell's declared range.
    #[error("qualification cell {cell} run attempt {attempt} exceeds declared attempts {attempts}")]
    RunAttemptOutOfRange {
        /// Cell containing the run.
        cell: CellId,
        /// Run attempt ordinal.
        attempt: u16,
        /// Declared cell attempts.
        attempts: u16,
    },
    /// A declared attempt had no sealed scenario evidence.
    #[error("qualification evidence cell {cell} is missing attempt {attempt}")]
    AttemptMissing {
        /// Cell missing evidence.
        cell: CellId,
        /// Missing one-based attempt ordinal.
        attempt: u16,
    },
    /// A scenario run identity appeared more than once.
    #[error("duplicate qualification evidence run {0}")]
    DuplicateRun(RunId),
    /// A referenced evidence path was not portable and relative.
    #[error("invalid qualification evidence path {0}")]
    EvidencePathInvalid(String),
    /// The path did not match the cell and run identities.
    #[error("qualification run {run} path {actual} does not match {expected}")]
    EvidencePathMismatch {
        /// Run whose path disagreed.
        run: RunId,
        /// Deterministic expected path.
        expected: String,
        /// Recorded path.
        actual: String,
    },
    /// A cell status disagreed with its run verdicts.
    #[error("cell {cell} status {actual:?} does not match {expected:?}")]
    CellStatusMismatch {
        /// Mismatched cell.
        cell: CellId,
        /// Derived status.
        expected: VerdictStatus,
        /// Recorded status.
        actual: VerdictStatus,
    },
    /// The top-level status disagreed with gating cell verdicts.
    #[error("qualification status {actual:?} does not match {expected:?}")]
    StatusMismatch {
        /// Derived status.
        expected: VerdictStatus,
        /// Recorded status.
        actual: VerdictStatus,
    },
}
