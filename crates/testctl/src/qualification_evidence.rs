//! Qualification evidence sealing atomically publishes a recursively digested tree.

use std::fmt::Write as _;
use std::fs;
use std::path::{Path, PathBuf};

use testlab_schema::{
    QualificationEvidenceManifest, QualificationManifest, RunId, SubjectManifest,
};

use crate::evidence_io::{
    digest_tree, sync_directory, sync_tree_directories, write_bytes, write_json,
};
use crate::run_error::AppError;

#[derive(Debug)]
pub(crate) struct QualificationEvidenceDirectory {
    evidence_root: PathBuf,
    partial: PathBuf,
    final_path: PathBuf,
}

impl QualificationEvidenceDirectory {
    pub(crate) fn begin(
        repository_root: &Path,
        evidence_directory: &Path,
        run_id: &RunId,
    ) -> Result<Self, AppError> {
        let evidence_root = absolute(repository_root, evidence_directory);
        fs::create_dir_all(&evidence_root).map_err(|error| {
            AppError::io(
                format!("failed to create {}", evidence_root.display()),
                error,
            )
        })?;
        let partial = evidence_root.join(format!("{run_id}.partial"));
        let final_path = evidence_root.join(run_id.to_string());
        if partial.exists() || final_path.exists() {
            return Err(AppError::Evidence(format!(
                "qualification path already exists for {run_id}"
            )));
        }
        fs::create_dir(&partial).map_err(|error| {
            AppError::io(format!("failed to create {}", partial.display()), error)
        })?;
        fs::create_dir(partial.join("cells")).map_err(|error| {
            AppError::io("failed to create qualification cells directory", error)
        })?;
        Ok(Self {
            evidence_root,
            partial,
            final_path,
        })
    }

    pub(crate) fn cell_directory(&self, cell_id: &str) -> Result<PathBuf, AppError> {
        let directory = self.partial.join("cells").join(cell_id);
        fs::create_dir(&directory).map_err(|error| {
            AppError::io(
                format!("failed to create qualification cell {cell_id}"),
                error,
            )
        })?;
        Ok(directory)
    }

    pub(crate) fn seal(self, request: &QualificationSealRequest<'_>) -> Result<PathBuf, AppError> {
        request
            .manifest
            .validate()
            .map_err(|error| AppError::Evidence(error.to_string()))?;
        write_json(&self.partial, "manifest.json", request.manifest)?;
        write_json(&self.partial, "qualification.json", request.qualification)?;
        write_json(&self.partial, "subject.json", request.subject)?;
        write_bytes(
            &self.partial,
            "summary.md",
            summary(request.manifest)?.as_bytes(),
            false,
        )?;
        write_bytes(
            &self.partial,
            "reproduction.sh",
            reproduction(request).as_bytes(),
            true,
        )?;
        let digests = digest_tree(&self.partial)?;
        write_json(&self.partial, "digests.json", &digests)?;
        sync_tree_directories(&self.partial)?;
        fs::rename(&self.partial, &self.final_path).map_err(|error| {
            AppError::io(
                format!(
                    "failed to publish {} as {}",
                    self.partial.display(),
                    self.final_path.display()
                ),
                error,
            )
        })?;
        sync_directory(&self.evidence_root)?;
        Ok(self.final_path)
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct QualificationSealRequest<'a> {
    pub(crate) repository_root: &'a Path,
    pub(crate) qualification_path: &'a Path,
    pub(crate) subject_path: &'a Path,
    pub(crate) qualification: &'a QualificationManifest,
    pub(crate) subject: &'a SubjectManifest,
    pub(crate) manifest: &'a QualificationEvidenceManifest,
    pub(crate) cell: Option<&'a str>,
    pub(crate) shards: &'a [PathBuf],
}

fn summary(manifest: &QualificationEvidenceManifest) -> Result<String, AppError> {
    let mut text = format!(
        "# Testlab qualification\n\n- Run: `{}`\n- Qualification: `{}`\n- Subject: `{}`\n- Status: `{:?}`\n\n## Cells\n",
        manifest.run_id, manifest.qualification_id, manifest.subject_id, manifest.status
    );
    for cell in &manifest.cells {
        writeln!(
            &mut text,
            "\n- `{}`: `{:?}` (gating: {}, attempts: {}, runs: {})\n",
            cell.cell_id,
            cell.status,
            cell.gating,
            cell.attempts,
            cell.runs.len()
        )
        .map_err(|error| AppError::Evidence(format!("failed to render summary: {error}")))?;
    }
    Ok(text)
}

fn reproduction(request: &QualificationSealRequest<'_>) -> String {
    let root = shell_quote(&request.repository_root.display().to_string());
    let qualification = shell_quote(&request.qualification_path.display().to_string());
    let subject = shell_quote(&request.subject_path.display().to_string());
    if !request.shards.is_empty() {
        let shards = request
            .shards
            .iter()
            .map(|path| format!(" --shard {}", shell_quote(&path.display().to_string())))
            .collect::<String>();
        return format!(
            "#!/usr/bin/env bash\nset -euo pipefail\nrepo_root={root}\nexec \"$repo_root/target/debug/testctl\" aggregate-qualification --root \"$repo_root\" --qualification {qualification}{shards} --evidence-dir \"$repo_root/evidence\"\n"
        );
    }
    let cell = request
        .cell
        .map_or_else(String::new, |cell| format!(" --cell {}", shell_quote(cell)));
    format!(
        "#!/usr/bin/env bash\nset -euo pipefail\nrepo_root={root}\nexec \"$repo_root/target/debug/testctl\" qualify --root \"$repo_root\" --qualification {qualification} --subject {subject}{cell} --evidence-dir \"$repo_root/evidence\"\n"
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
