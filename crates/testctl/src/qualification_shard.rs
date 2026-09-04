//! Shard verification binds complete scenario attempts to reviewed catalogs and package bytes.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use serde::de::DeserializeOwned;
use testlab_schema::{
    EVIDENCE_SCHEMA_VERSION, EnvironmentManifest, EvidenceManifest, QualificationEvidenceManifest,
    QualificationManifest, Scenario, SubjectManifest, Verdict,
};

use crate::catalog::Repository;
use crate::evidence_io::digest_tree;
use crate::qualification::select_qualification;
use crate::run_error::AppError;

pub(crate) struct VerifiedShard {
    pub(crate) directory: PathBuf,
    pub(crate) manifest: QualificationEvidenceManifest,
    pub(crate) subject: SubjectManifest,
}

pub(crate) fn verify_shard(
    repository: &Repository,
    qualification: &QualificationManifest,
    directory: &Path,
) -> Result<VerifiedShard, AppError> {
    require(
        !directory.to_string_lossy().ends_with(".partial"),
        "partial qualification cannot be aggregated",
    )?;
    let recorded: BTreeMap<String, String> = read_json(&directory.join("digests.json"))?;
    require(recorded == digest_tree(directory)?, "shard digest mismatch")?;
    let manifest: QualificationEvidenceManifest = read_json(&directory.join("manifest.json"))?;
    manifest
        .validate()
        .map_err(|error| AppError::Evidence(error.to_string()))?;
    require(
        manifest.cells.len() == 1,
        "expected exactly one cell per shard",
    )?;
    let cell = &manifest.cells[0];
    let selected = select_qualification(qualification, Some(cell.cell_id.as_str()))?;
    let archived: QualificationManifest = read_json(&directory.join("qualification.json"))?;
    require(
        archived == selected,
        "shard qualification differs from reviewed cell",
    )?;
    require(
        manifest.qualification_id == selected.id,
        "shard qualification identity mismatch",
    )?;
    let subject: SubjectManifest = read_json(&directory.join("subject.json"))?;
    subject
        .validate()
        .map_err(|error| AppError::Evidence(error.to_string()))?;
    require(
        manifest.subject_id == subject.id,
        "shard subject identity mismatch",
    )?;
    let expected = &selected.cells[0];
    let (_, environment) = repository.load_environment(Path::new(&expected.environment))?;
    let (_, pack) = repository.load_pack(Path::new(&expected.pack))?;
    require(
        cell.environment_id == environment.id
            && cell.pack_id == pack.id
            && cell.attempts == expected.attempts
            && cell.gating == expected.gating,
        "shard cell configuration mismatch",
    )?;
    let scenarios = pack
        .scenarios
        .iter()
        .map(|path| {
            repository
                .load_scenario(Path::new(path))
                .map(|(_, scenario)| scenario)
        })
        .collect::<Result<Vec<_>, _>>()?;
    require(
        cell.runs.len() == scenarios.len() * usize::from(expected.attempts),
        "shard scenario count mismatch",
    )?;
    for (ordinal, run) in cell.runs.iter().enumerate() {
        let scenario = &scenarios[ordinal % scenarios.len()];
        require(
            usize::from(run.attempt) == ordinal / scenarios.len() + 1
                && run.scenario_id == scenario.id,
            "shard scenario or attempt order mismatch",
        )?;
        let root = directory.join(&run.evidence_path);
        let evidence: EvidenceManifest = read_json(&root.join("manifest.json"))?;
        let verdict: Verdict = read_json(&root.join("verdict.json"))?;
        require(
            evidence.schema_version == EVIDENCE_SCHEMA_VERSION
                && evidence.run_id == run.run_id
                && evidence.scenario_id == scenario.id
                && evidence.environment_id == environment.id
                && evidence.subject_id == subject.id
                && evidence.status == run.status
                && verdict.status == run.status
                && evidence.completed_unix_ms >= evidence.started_unix_ms
                && evidence.started_unix_ms >= manifest.started_unix_ms
                && evidence.completed_unix_ms <= manifest.completed_unix_ms,
            "shard scenario evidence mismatch",
        )?;
        require(
            read_json::<Scenario>(&root.join("scenario.json"))? == *scenario,
            "shard scenario definition mismatch",
        )?;
        require(
            read_json::<EnvironmentManifest>(&root.join("environment.json"))? == environment,
            "shard environment definition mismatch",
        )?;
        require(
            read_json::<SubjectManifest>(&root.join("subject.json"))? == subject,
            "shard scenario subject mismatch",
        )?;
    }
    Ok(VerifiedShard {
        directory: directory.to_path_buf(),
        manifest,
        subject,
    })
}

pub(crate) fn same_candidate(left: &SubjectManifest, right: &SubjectManifest) -> bool {
    if left.artifacts.is_empty() || right.artifacts.is_empty() {
        return left == right;
    }
    let mut normalized = right.clone();
    // Independent runners build the same packaged bytes under different temporary paths.
    normalized.command.clone_from(&left.command);
    normalized == *left
}

pub(crate) fn read_json<T: DeserializeOwned>(path: &Path) -> Result<T, AppError> {
    let bytes =
        fs::read(path).map_err(|error| AppError::io(format!("read {}", path.display()), error))?;
    serde_json::from_slice(&bytes)
        .map_err(|error| AppError::json(path.display().to_string(), error))
}

pub(crate) fn require(condition: bool, message: &str) -> Result<(), AppError> {
    if condition {
        Ok(())
    } else {
        Err(AppError::Evidence(message.to_owned()))
    }
}
