//! Qualification evidence tests prove fail-closed deterministic aggregation.

use crate::{
    CellId, EnvironmentId, PackId, QualificationCellEvidence, QualificationEvidenceError,
    QualificationEvidenceManifest, QualificationId, QualificationRunEvidence, RunId, ScenarioId,
    SubjectId, VerdictStatus,
};

#[test]
fn invalid_gating_run_makes_the_qualification_invalid() {
    let mut evidence = fixture();
    evidence.cells[0].runs[0].status = VerdictStatus::Invalid;
    evidence.cells[0].status = VerdictStatus::Invalid;
    evidence.status = VerdictStatus::Invalid;

    assert_eq!(evidence.validate(), Ok(()));
}

#[test]
fn invalid_non_gating_cell_does_not_block_a_pass() {
    let mut evidence = fixture();
    let mut advisory = evidence.cells[0].clone();
    advisory.cell_id = cell_id("advisory");
    advisory.gating = false;
    advisory.status = VerdictStatus::Invalid;
    advisory.runs[0].run_id = run_id("scenario-run-2");
    advisory.runs[0].status = VerdictStatus::Invalid;
    advisory.runs[0].evidence_path = "cells/advisory/scenario-run-2".to_owned();
    evidence.cells.push(advisory);

    assert_eq!(evidence.validate(), Ok(()));
}

#[test]
fn manufactured_pass_is_rejected() {
    let mut evidence = fixture();
    evidence.cells[0].runs[0].status = VerdictStatus::Failed;

    assert_eq!(
        evidence.validate(),
        Err(QualificationEvidenceError::CellStatusMismatch {
            cell: cell_id("gating"),
            expected: VerdictStatus::Failed,
            actual: VerdictStatus::Passed,
        })
    );
}

#[test]
fn every_declared_attempt_requires_evidence() {
    let mut evidence = fixture();
    evidence.cells[0].attempts = 2;

    assert_eq!(
        evidence.validate(),
        Err(QualificationEvidenceError::AttemptMissing {
            cell: cell_id("gating"),
            attempt: 2,
        })
    );
}

#[test]
fn one_failed_attempt_fails_the_cell() {
    let mut evidence = fixture();
    let mut second = evidence.cells[0].runs[0].clone();
    second.attempt = 2;
    second.run_id = run_id("scenario-run-2");
    second.status = VerdictStatus::Failed;
    second.evidence_path = "cells/gating/scenario-run-2".to_owned();
    evidence.cells[0].attempts = 2;
    evidence.cells[0].runs.push(second);
    evidence.cells[0].status = VerdictStatus::Failed;
    evidence.status = VerdictStatus::Failed;

    assert_eq!(evidence.validate(), Ok(()));
    assert_eq!(
        QualificationCellEvidence::aggregate_status(&evidence.cells[0].runs),
        VerdictStatus::Failed
    );
}

fn fixture() -> QualificationEvidenceManifest {
    QualificationEvidenceManifest {
        schema_version: 2,
        run_id: run_id("qualification-run-1"),
        qualification_id: QualificationId::new("repository-pr")
            .unwrap_or_else(|error| panic!("qualification id: {error}")),
        subject_id: SubjectId::new("reference-rust")
            .unwrap_or_else(|error| panic!("subject id: {error}")),
        started_unix_ms: 1,
        completed_unix_ms: 2,
        status: VerdictStatus::Passed,
        cells: vec![QualificationCellEvidence {
            cell_id: cell_id("gating"),
            environment_id: EnvironmentId::new("model-broker")
                .unwrap_or_else(|error| panic!("environment id: {error}")),
            pack_id: PackId::new("repository-pr")
                .unwrap_or_else(|error| panic!("pack id: {error}")),
            attempts: 1,
            gating: true,
            status: VerdictStatus::Passed,
            runs: vec![QualificationRunEvidence {
                attempt: 1,
                run_id: run_id("scenario-run-1"),
                scenario_id: ScenarioId::new("producer.round-trip")
                    .unwrap_or_else(|error| panic!("scenario id: {error}")),
                status: VerdictStatus::Passed,
                evidence_path: "cells/gating/scenario-run-1".to_owned(),
            }],
        }],
    }
}

fn cell_id(value: &str) -> CellId {
    CellId::new(value).unwrap_or_else(|error| panic!("cell id: {error}"))
}

fn run_id(value: &str) -> RunId {
    RunId::new(value).unwrap_or_else(|error| panic!("run id: {error}"))
}
