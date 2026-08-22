//! Evidence sealing publishes a run only after every required artifact is durable.

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};
use testlab_schema::{
    AdapterDescriptor, BrokerObservation, EvidenceManifest, HistoryEntry, RunId, Scenario,
    SubjectManifest, Verdict,
};

use crate::run_error::AppError;

#[derive(Debug)]
pub(crate) struct SealRequest<'a> {
    pub(crate) repository_root: &'a Path,
    pub(crate) evidence_directory: &'a Path,
    pub(crate) scenario_path: &'a Path,
    pub(crate) subject_path: &'a Path,
    pub(crate) run_id: &'a RunId,
    pub(crate) scenario: &'a Scenario,
    pub(crate) subject: &'a SubjectManifest,
    pub(crate) adapter: Option<&'a AdapterDescriptor>,
    pub(crate) history: &'a [HistoryEntry],
    pub(crate) observations: &'a [BrokerObservation],
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
        schema_version: 1,
        run_id: request.run_id.clone(),
        scenario_id: request.scenario.id.clone(),
        subject_id: request.subject.id.clone(),
        started_unix_ms: request.started_unix_ms,
        completed_unix_ms: request.completed_unix_ms,
        adapter: request.adapter.cloned(),
        status: request.verdict.status,
    };
    write_json(directory, "manifest.json", &manifest)?;
    write_json(directory, "scenario.json", request.scenario)?;
    write_json(directory, "subject.json", request.subject)?;
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

fn write_json<T: serde::Serialize + ?Sized>(
    directory: &Path,
    name: &str,
    value: &T,
) -> Result<(), AppError> {
    let mut bytes =
        serde_json::to_vec_pretty(value).map_err(|error| AppError::json(name, error))?;
    bytes.push(b'\n');
    write_bytes(directory, name, &bytes, false)
}

fn write_json_lines<T: serde::Serialize>(
    directory: &Path,
    name: &str,
    values: &[T],
) -> Result<(), AppError> {
    let path = directory.join(name);
    let mut file = create_new(&path)?;
    for value in values {
        serde_json::to_writer(&mut file, value).map_err(|error| AppError::json(name, error))?;
        file.write_all(b"\n")
            .map_err(|error| AppError::io(format!("failed to write {}", path.display()), error))?;
    }
    file.sync_all()
        .map_err(|error| AppError::io(format!("failed to sync {}", path.display()), error))
}

fn write_bytes(
    directory: &Path,
    name: &str,
    bytes: &[u8],
    executable: bool,
) -> Result<(), AppError> {
    let path = directory.join(name);
    let mut file = create_new(&path)?;
    file.write_all(bytes)
        .map_err(|error| AppError::io(format!("failed to write {}", path.display()), error))?;
    file.sync_all()
        .map_err(|error| AppError::io(format!("failed to sync {}", path.display()), error))?;
    if executable {
        make_executable(&path)?;
    }
    Ok(())
}

fn create_new(path: &Path) -> Result<File, AppError> {
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|error| AppError::io(format!("failed to create {}", path.display()), error))
}

fn digest_directory(directory: &Path) -> Result<BTreeMap<String, String>, AppError> {
    let mut paths = fs::read_dir(directory)
        .map_err(|error| AppError::io(format!("failed to list {}", directory.display()), error))?
        .map(|entry| entry.map(|entry| entry.path()))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| AppError::io("failed to inspect evidence files", error))?;
    paths.sort();
    let mut digests = BTreeMap::new();
    for path in paths {
        if path.file_name() == Some(std::ffi::OsStr::new("digests.json")) {
            continue;
        }
        let name = path
            .file_name()
            .and_then(std::ffi::OsStr::to_str)
            .ok_or_else(|| AppError::Evidence("non-UTF-8 evidence name".to_owned()))?;
        if digests
            .insert(name.to_owned(), digest_file(&path)?)
            .is_some()
        {
            return Err(AppError::Evidence(format!(
                "duplicate evidence artifact name {name}"
            )));
        }
    }
    Ok(digests)
}

fn digest_file(path: &Path) -> Result<String, AppError> {
    let mut file = File::open(path)
        .map_err(|error| AppError::io(format!("failed to open {}", path.display()), error))?;
    let mut digest = Sha256::new();
    let mut buffer = vec![0_u8; 64 * 1024];
    loop {
        let bytes = file
            .read(&mut buffer)
            .map_err(|error| AppError::io(format!("failed to hash {}", path.display()), error))?;
        if bytes == 0 {
            break;
        }
        digest.update(&buffer[..bytes]);
    }
    Ok(hex::encode(digest.finalize()))
}

fn summary(request: &SealRequest<'_>) -> String {
    let mut text = format!(
        "# Testlab result\n\n- Run: `{}`\n- Scenario: `{}`\n- Subject: `{}`\n- Status: `{:?}`\n",
        request.run_id, request.scenario.id, request.subject.id, request.verdict.status
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
    format!(
        "#!/usr/bin/env bash\nset -euo pipefail\nrepo_root={root}\nexec \"$repo_root/target/debug/testctl\" run --root \"$repo_root\" --scenario {scenario} --subject {subject} --evidence-dir \"$repo_root/evidence\"\n"
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

#[cfg(unix)]
fn make_executable(path: &Path) -> Result<(), AppError> {
    use std::os::unix::fs::PermissionsExt;
    let mut permissions = fs::metadata(path)
        .map_err(|error| AppError::io("failed to inspect reproduction script", error))?
        .permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(path, permissions)
        .map_err(|error| AppError::io("failed to mark reproduction script executable", error))
}

#[cfg(not(unix))]
fn make_executable(_path: &Path) -> Result<(), AppError> {
    Ok(())
}

#[cfg(unix)]
fn sync_directory(path: &Path) -> Result<(), AppError> {
    File::open(path)
        .and_then(|file| file.sync_all())
        .map_err(|error| AppError::io(format!("failed to sync {}", path.display()), error))
}

#[cfg(not(unix))]
fn sync_directory(_path: &Path) -> Result<(), AppError> {
    Ok(())
}
