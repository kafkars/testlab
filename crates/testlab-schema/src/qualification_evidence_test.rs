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

fn fixture() -> QualificationEvidenceManifest {
    QualificationEvidenceManifest {
        schema_version: 1,
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
            gating: true,
            status: VerdictStatus::Passed,
            runs: vec![QualificationRunEvidence {
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
