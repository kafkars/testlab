//! Qualification execution runs every reviewed cell and seals one aggregate result.

use std::path::Path;

use testlab_schema::{
    QUALIFICATION_EVIDENCE_SCHEMA_VERSION, QualificationCellEvidence,
    QualificationEvidenceManifest, QualificationRunEvidence, VerdictStatus,
};

use crate::catalog::Repository;
use crate::identity::new_run_id;
use crate::qualification_evidence::{QualificationEvidenceDirectory, QualificationSealRequest};
use crate::run_error::AppError;
use crate::runner::run_pack;
use crate::time::unix_ms;

#[derive(Clone, Debug)]
pub(crate) struct QualificationRun {
    pub(crate) path: std::path::PathBuf,
    pub(crate) status: VerdictStatus,
}

pub(crate) fn run_qualification(
    repository: &Repository,
    qualification_path: &Path,
    subject_path: &Path,
    evidence_directory: &Path,
) -> Result<QualificationRun, AppError> {
    repository.validate_all()?;
    let (qualification_path, qualification) = repository.load_qualification(qualification_path)?;
    let (subject_path, subject) = repository.load_subject(subject_path)?;
    let started_unix_ms = qualification_time()?;
    let run_id = new_run_id("qualification", started_unix_ms)?;
    let evidence =
        QualificationEvidenceDirectory::begin(repository.root(), evidence_directory, &run_id)?;
    let mut cells = Vec::with_capacity(qualification.cells.len());
    for cell in &qualification.cells {
        let (_, environment) = repository.load_environment(Path::new(&cell.environment))?;
        let (_, pack) = repository.load_pack(Path::new(&cell.pack))?;
        let cell_directory = evidence.cell_directory(cell.id.as_str())?;
        let sealed_runs = run_pack(
            repository,
            Path::new(&cell.pack),
            &subject_path,
            Path::new(&cell.environment),
            &cell_directory,
        )?;
        let runs = sealed_runs
            .into_iter()
            .map(|run| QualificationRunEvidence {
                evidence_path: format!("cells/{}/{}", cell.id, run.run_id),
                run_id: run.run_id,
                scenario_id: run.scenario_id,
                status: run.verdict.status,
            })
            .collect::<Vec<_>>();
        let status = QualificationCellEvidence::aggregate_status(&runs);
        cells.push(QualificationCellEvidence {
            cell_id: cell.id.clone(),
            environment_id: environment.id,
            pack_id: pack.id,
            gating: cell.gating,
            status,
            runs,
        });
    }
    let completed_unix_ms = qualification_time()?;
    let status = QualificationEvidenceManifest::aggregate_status(&cells);
    let manifest = QualificationEvidenceManifest {
        schema_version: QUALIFICATION_EVIDENCE_SCHEMA_VERSION,
        run_id,
        qualification_id: qualification.id.clone(),
        subject_id: subject.id.clone(),
        started_unix_ms,
        completed_unix_ms,
        status,
        cells,
    };
    let path = evidence.seal(&QualificationSealRequest {
        repository_root: repository.root(),
        qualification_path: &qualification_path,
        subject_path: &subject_path,
        qualification: &qualification,
        subject: &subject,
        manifest: &manifest,
    })?;
    Ok(QualificationRun { path, status })
}

fn qualification_time() -> Result<u64, AppError> {
    unix_ms().map_err(|error| AppError::Catalog(error.to_string()))
}
