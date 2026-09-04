//! Qualification merging requires every reviewed cell and identical candidate artifacts.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use testlab_schema::{QUALIFICATION_EVIDENCE_SCHEMA_VERSION, QualificationEvidenceManifest};

use crate::catalog::Repository;
use crate::identity::new_run_id;
use crate::qualification::QualificationRun;
use crate::qualification_evidence::{QualificationEvidenceDirectory, QualificationSealRequest};
use crate::qualification_shard::{require, same_candidate, verify_shard};
use crate::run_error::AppError;
use crate::time::unix_ms;

pub(crate) fn aggregate_qualification(
    repository: &Repository,
    qualification_path: &Path,
    shard_paths: &[PathBuf],
    evidence_directory: &Path,
) -> Result<QualificationRun, AppError> {
    repository.validate_all()?;
    let (qualification_path, qualification) = repository.load_qualification(qualification_path)?;
    require(
        shard_paths.len() == qualification.cells.len(),
        "missing or extra qualification shards",
    )?;
    let mut shards = BTreeMap::new();
    for path in shard_paths {
        let shard = verify_shard(repository, &qualification, path)?;
        let identity = shard.manifest.cells[0].cell_id.clone();
        require(
            shards.insert(identity, shard).is_none(),
            "duplicate qualification cell shard",
        )?;
    }
    let first = shards
        .values()
        .next()
        .ok_or_else(|| AppError::Evidence("no shards".to_owned()))?;
    let subject = first.subject.clone();
    let mut started_unix_ms = first.manifest.started_unix_ms;
    let mut completed_unix_ms = first.manifest.completed_unix_ms;
    let mut cells = Vec::new();
    for expected in &qualification.cells {
        let shard = shards.get(&expected.id).ok_or_else(|| {
            AppError::Evidence(format!("missing qualification cell {}", expected.id))
        })?;
        require(
            same_candidate(&subject, &shard.subject),
            "qualification candidate artifacts differ",
        )?;
        started_unix_ms = started_unix_ms.min(shard.manifest.started_unix_ms);
        completed_unix_ms = completed_unix_ms.max(shard.manifest.completed_unix_ms);
        cells.push(shard.manifest.cells[0].clone());
    }
    let now = unix_ms().map_err(|error| AppError::Evidence(error.to_string()))?;
    let run_id = new_run_id("qualification", now)?;
    let status = QualificationEvidenceManifest::aggregate_status(&cells);
    let manifest = QualificationEvidenceManifest {
        schema_version: QUALIFICATION_EVIDENCE_SCHEMA_VERSION,
        run_id: run_id.clone(),
        qualification_id: qualification.id.clone(),
        subject_id: subject.id.clone(),
        started_unix_ms,
        completed_unix_ms,
        status,
        cells,
    };
    manifest
        .validate()
        .map_err(|error| AppError::Evidence(error.to_string()))?;
    let evidence =
        QualificationEvidenceDirectory::begin(repository.root(), evidence_directory, &run_id)?;
    for cell in &manifest.cells {
        let shard = &shards[&cell.cell_id];
        let destination = evidence.cell_directory(cell.cell_id.as_str())?;
        copy_contents(
            &shard.directory.join("cells").join(cell.cell_id.as_str()),
            &destination,
        )?;
    }
    let path = evidence.seal(&QualificationSealRequest {
        repository_root: repository.root(),
        qualification_path: &qualification_path,
        subject_path: Path::new("subject.toml"),
        qualification: &qualification,
        subject: &subject,
        manifest: &manifest,
        cell: None,
        shards: shard_paths,
    })?;
    Ok(QualificationRun { path, status })
}

fn copy_contents(source: &Path, destination: &Path) -> Result<(), AppError> {
    for entry in
        fs::read_dir(source).map_err(|error| AppError::io("read shard directory", error))?
    {
        let entry = entry.map_err(|error| AppError::io("read shard entry", error))?;
        let kind = entry
            .file_type()
            .map_err(|error| AppError::io("inspect shard entry", error))?;
        let target = destination.join(entry.file_name());
        if kind.is_dir() {
            fs::create_dir(&target)
                .map_err(|error| AppError::io("create aggregate directory", error))?;
            copy_contents(&entry.path(), &target)?;
        } else if kind.is_file() {
            fs::copy(entry.path(), target)
                .map_err(|error| AppError::io("copy shard evidence", error))?;
        } else {
            return Err(AppError::Evidence("unsupported shard entry".to_owned()));
        }
    }
    Ok(())
}
