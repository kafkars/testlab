//! Candidate manifests bind the adapter and subject to extracted package bytes.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};
use testlab_schema::{SUBJECT_SCHEMA_VERSION, SubjectArtifact, SubjectId, SubjectManifest};

use crate::run_error::AppError;

#[derive(Clone, Debug)]
pub(crate) struct PackageArtifact {
    pub(crate) name: String,
    pub(crate) version: String,
    pub(crate) digest: String,
    pub(crate) source: PackageSource,
}

#[derive(Clone, Debug)]
pub(crate) enum PackageSource {
    Extracted(PathBuf),
    Registry,
}

pub(crate) fn is_complete_semver(version: &str) -> bool {
    let (version, build) = version
        .split_once('+')
        .map_or((version, None), |(version, build)| (version, Some(build)));
    if build.is_some_and(|build| !valid_identifiers(build, false)) {
        return false;
    }
    let (core, prerelease) = version
        .split_once('-')
        .map_or((version, None), |(core, prerelease)| {
            (core, Some(prerelease))
        });
    if prerelease.is_some_and(|prerelease| !valid_identifiers(prerelease, true)) {
        return false;
    }
    let mut core = core.split('.');
    let components = [core.next(), core.next(), core.next()];
    core.next().is_none()
        && components
            .into_iter()
            .all(|component| component.is_some_and(valid_core_number))
}

fn valid_core_number(value: &str) -> bool {
    !value.is_empty()
        && value.bytes().all(|byte| byte.is_ascii_digit())
        && (value.len() == 1 || !value.starts_with('0'))
        && value.parse::<u64>().is_ok()
}

fn valid_identifiers(value: &str, reject_numeric_zeroes: bool) -> bool {
    !value.is_empty()
        && value.split('.').all(|identifier| {
            !identifier.is_empty()
                && identifier
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
                && (!reject_numeric_zeroes
                    || !identifier.bytes().all(|byte| byte.is_ascii_digit())
                    || identifier.len() == 1
                    || !identifier.starts_with('0'))
        })
}

pub(crate) fn write_adapter_manifest(
    repository_root: &Path,
    directory: &Path,
    artifacts: &[PackageArtifact],
) -> Result<PathBuf, AppError> {
    let adapter = directory.join("adapter");
    fs::create_dir_all(&adapter)
        .map_err(|error| AppError::io(format!("failed to create {}", adapter.display()), error))?;
    let source = adapter_manifest(repository_root, artifacts)?;
    let path = adapter.join("Cargo.toml");
    fs::write(&path, source)
        .map_err(|error| AppError::io(format!("failed to write {}", path.display()), error))?;
    Ok(path)
}

pub(crate) fn adapter_manifest(
    root: &Path,
    artifacts: &[PackageArtifact],
) -> Result<String, AppError> {
    let source = |name: &str| -> Result<String, AppError> {
        artifacts
            .iter()
            .find(|artifact| artifact.name == name)
            .ok_or_else(|| AppError::Candidate(format!("missing packaged source {name}")))
            .and_then(|artifact| match &artifact.source {
                PackageSource::Extracted(path) => toml_path(path),
                PackageSource::Registry => Err(AppError::Candidate(format!(
                    "packaged source {name} unexpectedly resolved from a registry"
                ))),
            })
    };
    let support_patches = support_patches(artifacts, &source)?;
    Ok(format!(
        include_str!("candidate_adapter.toml"),
        adapter_lib = toml_path(&root.join("adapters/kafkars-rust/src/lib.rs"))?,
        adapter_bin = toml_path(&root.join("adapters/kafkars-rust/src/main.rs"))?,
        schema = toml_path(&root.join("crates/testlab-schema"))?,
        core = source("kafka-client-core")?,
        engine = source("kafka-client-engine")?,
        kafkars = source("kafkars")?,
        support_patches = support_patches,
    ))
}

fn support_patches(
    artifacts: &[PackageArtifact],
    source: &impl Fn(&str) -> Result<String, AppError>,
) -> Result<String, AppError> {
    let support = [
        "kafka-driver",
        "kafka-driver-core",
        "kafka-driver-transport",
        "kafka-wire",
        "kafka-wire-core",
        "kafka-wire-records",
    ];
    let registry_count = support
        .iter()
        .filter(|name| {
            artifacts.iter().any(|artifact| {
                artifact.name == **name && matches!(&artifact.source, PackageSource::Registry)
            })
        })
        .count();
    if registry_count == support.len() {
        return Ok(String::new());
    }
    if registry_count != 0 {
        return Err(AppError::Candidate(
            "driver and wire package sources must use one provenance mode".to_owned(),
        ));
    }
    support
        .iter()
        .map(|name| Ok(format!("{name} = {{ path = {} }}\n", source(name)?)))
        .collect::<Result<String, AppError>>()
}

pub(crate) fn write_subject(
    root: &Path,
    directory: &Path,
    artifacts: &[PackageArtifact],
) -> Result<PathBuf, AppError> {
    let digest = bundle_digest(artifacts);
    let version = artifacts
        .iter()
        .find(|artifact| artifact.name == "kafkars")
        .map(|artifact| artifact.version.as_str())
        .ok_or_else(|| AppError::Candidate("missing packaged Kafkars artifact".to_owned()))?;
    let command = directory.join("adapter-target/debug/testlab-kafkars-adapter");
    let relative_command = command.strip_prefix(root).map_err(|_| {
        AppError::Candidate("candidate adapter escaped the repository root".to_owned())
    })?;
    let subject = SubjectManifest {
        schema_version: SUBJECT_SCHEMA_VERSION,
        id: SubjectId::new(format!("kafkars-{version}-{}", &digest[..16]))
            .map_err(|error| AppError::Candidate(error.to_string()))?,
        display_name: format!("packaged Kafkars candidate {version}"),
        artifacts: artifacts
            .iter()
            .map(|artifact| SubjectArtifact {
                name: artifact.name.clone(),
                version: artifact.version.clone(),
                sha256: artifact.digest.clone(),
            })
            .collect(),
        command: path_text(relative_command)?,
        args: Vec::new(),
        environment: BTreeMap::new(),
        pass_environment: Vec::new(),
        working_directory: Some(".".to_owned()),
    };
    subject
        .validate()
        .map_err(|error| AppError::Candidate(error.to_string()))?;
    let encoded = toml::to_string_pretty(&subject)
        .map_err(|error| AppError::Candidate(format!("serialize subject manifest: {error}")))?;
    let path = directory.join("subject.toml");
    fs::write(&path, encoded)
        .map_err(|error| AppError::io(format!("failed to write {}", path.display()), error))?;
    Ok(path)
}

pub(crate) fn bundle_digest(artifacts: &[PackageArtifact]) -> String {
    let mut hasher = Sha256::new();
    for artifact in artifacts {
        hasher.update(artifact.name.as_bytes());
        hasher.update([0]);
        hasher.update(artifact.version.as_bytes());
        hasher.update([0]);
        hasher.update(artifact.digest.as_bytes());
        hasher.update([0]);
    }
    hex::encode(hasher.finalize())
}

fn toml_path(path: &Path) -> Result<String, AppError> {
    Ok(toml::Value::String(path_text(path)?).to_string())
}

fn path_text(path: &Path) -> Result<String, AppError> {
    path.to_str()
        .map(str::to_owned)
        .ok_or_else(|| AppError::Candidate(format!("path is not UTF-8: {}", path.display())))
}
