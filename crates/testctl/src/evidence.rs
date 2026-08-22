//! Evidence sealing publishes a run only after every required artifact is durable.

use std::fs;
use std::path::{Component, Path, PathBuf};

use testlab_environment::ComposeArtifact;
use testlab_schema::{
    AdapterDescriptor, BrokerObservation, EVIDENCE_SCHEMA_VERSION, EnvironmentManifest,
    EvidenceManifest, HistoryEntry, RunId, Scenario, SubjectManifest, Verdict,
};

use crate::evidence_io::{
    digest_directory, sync_directory, write_bytes, write_json, write_json_lines,
};
use crate::run_error::AppError;

#[derive(Debug)]
pub(crate) struct SealRequest<'a> {
    pub(crate) repository_root: &'a Path,
    pub(crate) evidence_directory: &'a Path,
    pub(crate) scenario_path: &'a Path,
    pub(crate) subject_path: &'a Path,
    pub(crate) environment_path: &'a Path,
    pub(crate) run_id: &'a RunId,
    pub(crate) scenario: &'a Scenario,
    pub(crate) subject: &'a SubjectManifest,
    pub(crate) environment: &'a EnvironmentManifest,
    pub(crate) adapter: Option<&'a AdapterDescriptor>,
    pub(crate) history: &'a [HistoryEntry],
    pub(crate) observations: &'a [BrokerObservation],
    pub(crate) environment_artifacts: &'a [ComposeArtifact],
    pub(crate) verdict: &'a Verdict,
    pub(crate) started_unix_ms: u64,
    pub(crate) completed_unix_ms: u64,
}

#[derive(Clone, Debug)]
pub(crate) struct SealedRun {
    pub(crate) path: PathBuf,
    pub(crate) verdict: Verdict,
}

pub(crate) fn seal(request: &SealRequest<'_>) -> Result<SealedRun, AppError> {
    let evidence_root = absolute(request.repository_root, request.evidence_directory);
    fs::create_dir_all(&evidence_root).map_err(|error| {
        AppError::io(
            format!("failed to create {}", evidence_root.display()),
            error,
        )
    })?;
    let partial = evidence_root.join(format!("{}.partial", request.run_id));
    let final_path = evidence_root.join(request.run_id.to_string());
    if partial.exists() || final_path.exists() {
        return Err(AppError::Evidence(format!(
            "run path already exists for {}",
            request.run_id
        )));
    }
    fs::create_dir(&partial)
        .map_err(|error| AppError::io(format!("failed to create {}", partial.display()), error))?;
    write_artifacts(&partial, request)?;
    fs::rename(&partial, &final_path).map_err(|error| {
        AppError::io(
            format!(
                "failed to publish {} as {}",
                partial.display(),
                final_path.display()
            ),
            error,
        )
    })?;
    sync_directory(&evidence_root)?;
    Ok(SealedRun {
        path: final_path,
        verdict: request.verdict.clone(),
    })
}

fn write_artifacts(directory: &Path, request: &SealRequest<'_>) -> Result<(), AppError> {
    let manifest = EvidenceManifest {
        schema_version: EVIDENCE_SCHEMA_VERSION,
        run_id: request.run_id.clone(),
        scenario_id: request.scenario.id.clone(),
        subject_id: request.subject.id.clone(),
        environment_id: request.environment.id.clone(),
        started_unix_ms: request.started_unix_ms,
        completed_unix_ms: request.completed_unix_ms,
        adapter: request.adapter.cloned(),
        status: request.verdict.status,
    };
    write_json(directory, "manifest.json", &manifest)?;
    write_json(directory, "scenario.json", request.scenario)?;
    write_json(directory, "subject.json", request.subject)?;
    write_json(directory, "environment.json", request.environment)?;
    write_environment_artifacts(directory, request.environment_artifacts)?;
    if let Some(adapter) = request.adapter {
        write_json(directory, "adapter.json", adapter)?;
    }
    write_json_lines(directory, "history.jsonl", request.history)?;
    write_json_lines(directory, "broker-observations.jsonl", request.observations)?;
    write_json(directory, "verdict.json", request.verdict)?;
    write_bytes(directory, "summary.md", summary(request).as_bytes(), false)?;
    write_bytes(
        directory,
        "reproduction.sh",
        reproduction(request).as_bytes(),
        true,
    )?;
    let digests = digest_directory(directory)?;
    write_json(directory, "digests.json", &digests)?;
    sync_directory(directory)
}

fn write_environment_artifacts(
    directory: &Path,
    artifacts: &[ComposeArtifact],
) -> Result<(), AppError> {
    for artifact in artifacts {
        let path = Path::new(&artifact.name);
        let mut components = path.components();
        let portable = matches!(components.next(), Some(Component::Normal(_)))
            && components.next().is_none()
            && path.to_str().is_some()
            && !artifact.name.contains('/')
            && !artifact.name.contains('\\');
        if !portable {
            return Err(AppError::Evidence(format!(
                "invalid environment artifact name {}",
                artifact.name
            )));
        }
        write_bytes(directory, &artifact.name, &artifact.bytes, false)?;
    }
    Ok(())
}

fn summary(request: &SealRequest<'_>) -> String {
    let mut text = format!(
        "# Testlab result\n\n- Run: `{}`\n- Scenario: `{}`\n- Subject: `{}`\n- Environment: `{}`\n- Status: `{:?}`\n",
        request.run_id,
        request.scenario.id,
        request.subject.id,
        request.environment.id,
        request.verdict.status
    );
    if request.verdict.violations.is_empty() {
        text.push_str("\nNo deterministic contract violations.\n");
    } else {
        text.push_str("\n## Violations\n");
        for violation in &request.verdict.violations {
            text.push_str("\n- `");
            text.push_str(violation.contract_id.as_str());
            text.push_str("`: ");
            text.push_str(&violation.message);
            text.push('\n');
        }
    }
    text
}

fn reproduction(request: &SealRequest<'_>) -> String {
    let root = shell_quote(&request.repository_root.display().to_string());
    let scenario = shell_quote(&request.scenario_path.display().to_string());
    let subject = shell_quote(&request.subject_path.display().to_string());
    let environment = shell_quote(&request.environment_path.display().to_string());
    format!(
        "#!/usr/bin/env bash\nset -euo pipefail\nrepo_root={root}\nexec \"$repo_root/target/debug/testctl\" run --root \"$repo_root\" --scenario {scenario} --subject {subject} --environment {environment} --evidence-dir \"$repo_root/evidence\"\n"
    )
}

fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\"'\"'"))
}

fn absolute(root: &Path, path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        root.join(path)
    }
}
