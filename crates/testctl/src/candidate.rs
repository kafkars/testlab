//! Candidate preparation builds the adapter only from extracted Cargo packages.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use sha2::{Digest, Sha256};

use crate::candidate_manifest::{PackageArtifact, write_adapter_manifest, write_subject};
use crate::catalog::Repository;
use crate::identity::new_run_id;
use crate::run_error::AppError;
use crate::time::unix_ms;

const KAFKARS_PACKAGE_NAMES: [&str; 3] = ["kafka-client-core", "kafka-client-engine", "kafkars"];
const DRIVER_PACKAGE_NAMES: [&str; 3] = [
    "kafka-driver",
    "kafka-driver-core",
    "kafka-driver-transport",
];
const WIRE_PACKAGE_NAMES: [&str; 3] = ["kafka-wire", "kafka-wire-core", "kafka-wire-records"];
const PACKAGE_NAMES: [&str; 9] = [
    "kafka-client-core",
    "kafka-client-engine",
    "kafkars",
    "kafka-driver",
    "kafka-driver-core",
    "kafka-driver-transport",
    "kafka-wire",
    "kafka-wire-core",
    "kafka-wire-records",
];

#[derive(Debug)]
pub(crate) struct PreparedCandidate {
    pub(crate) directory: PathBuf,
    pub(crate) subject_path: PathBuf,
}

pub(crate) fn prepare_kafkars(
    repository: &Repository,
    kafkars_root: &Path,
    allow_dirty: bool,
) -> Result<PreparedCandidate, AppError> {
    let kafkars_root = canonical_directory(kafkars_root, "Kafkars root")?;
    let manifest = kafkars_root.join("Cargo.toml");
    if !manifest.is_file() {
        return Err(AppError::Candidate(format!(
            "missing Kafkars workspace manifest {}",
            manifest.display()
        )));
    }
    let identity = new_run_id("candidate", candidate_time()?)?;
    let directory = repository
        .root()
        .join("target/testlab-candidates")
        .join(identity.as_str());
    let package_target = directory.join("package-target");
    fs::create_dir_all(&package_target).map_err(|error| {
        AppError::io(
            format!("failed to create {}", package_target.display()),
            error,
        )
    })?;
    let sibling_root = kafkars_root.parent().ok_or_else(|| {
        AppError::Candidate("Kafkars root has no sibling dependency directory".to_owned())
    })?;
    let driver_manifest = sibling_manifest(sibling_root, "kafka-driver")?;
    let wire_manifest = sibling_manifest(sibling_root, "kafka-protocol")?;
    package(
        &manifest,
        &package_target,
        &KAFKARS_PACKAGE_NAMES,
        allow_dirty,
    )?;
    package(
        &driver_manifest,
        &package_target,
        &DRIVER_PACKAGE_NAMES,
        false,
    )?;
    package(&wire_manifest, &package_target, &WIRE_PACKAGE_NAMES, false)?;
    let artifacts = extract_packages(&directory, &package_target.join("package"))?;
    let adapter_manifest = write_adapter_manifest(repository.root(), &directory, &artifacts)?;
    build_adapter(&adapter_manifest, &directory.join("adapter-target"))?;
    let subject_path = write_subject(repository.root(), &directory, &artifacts)?;
    Ok(PreparedCandidate {
        directory,
        subject_path,
    })
}

fn package(
    manifest: &Path,
    target: &Path,
    package_names: &[&str],
    allow_dirty: bool,
) -> Result<(), AppError> {
    let mut command = Command::new("cargo");
    command
        .arg("package")
        .arg("--manifest-path")
        .arg(manifest)
        .arg("--target-dir")
        .arg(target)
        .arg("--locked")
        .arg("--no-verify");
    if allow_dirty {
        command.arg("--allow-dirty");
    }
    for name in package_names {
        command.arg("--package").arg(name);
    }
    run(&mut command, "package Kafkars public crates")
}

fn sibling_manifest(parent: &Path, sibling: &str) -> Result<PathBuf, AppError> {
    let root = canonical_directory(&parent.join(sibling), sibling)?;
    let manifest = root.join("Cargo.toml");
    if !manifest.is_file() {
        return Err(AppError::Candidate(format!(
            "missing {sibling} workspace manifest {}",
            manifest.display()
        )));
    }
    Ok(manifest)
}

fn extract_packages(
    directory: &Path,
    package_directory: &Path,
) -> Result<Vec<PackageArtifact>, AppError> {
    let sources = directory.join("sources");
    fs::create_dir_all(&sources)
        .map_err(|error| AppError::io(format!("failed to create {}", sources.display()), error))?;
    let mut artifacts = Vec::new();
    for name in PACKAGE_NAMES {
        let (archive, version) = find_archive(package_directory, name)?;
        let mut command = Command::new("tar");
        command
            .arg("--extract")
            .arg("--gzip")
            .arg("--file")
            .arg(&archive)
            .arg("--directory")
            .arg(&sources);
        run(&mut command, &format!("extract {name} package"))?;
        let extracted = sources.join(format!("{name}-{version}"));
        let source = sources.join(name);
        fs::rename(&extracted, &source).map_err(|error| {
            AppError::io(
                format!("failed to normalize {}", extracted.display()),
                error,
            )
        })?;
        artifacts.push(PackageArtifact {
            name: name.to_owned(),
            version,
            digest: digest(&archive)?,
            source,
        });
    }
    Ok(artifacts)
}

pub(crate) fn find_archive(directory: &Path, package: &str) -> Result<(PathBuf, String), AppError> {
    let prefix = format!("{package}-");
    let mut matches = Vec::new();
    let entries = fs::read_dir(directory)
        .map_err(|error| AppError::io(format!("failed to list {}", directory.display()), error))?;
    for entry in entries {
        let entry = entry.map_err(|error| {
            AppError::io(format!("failed to read {}", directory.display()), error)
        })?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if let Some(version) = name
            .strip_prefix(&prefix)
            .and_then(|value| value.strip_suffix(".crate"))
            .filter(|value| value.as_bytes().first().is_some_and(u8::is_ascii_digit))
        {
            matches.push((entry.path(), version.to_owned()));
        }
    }
    if matches.len() != 1 {
        return Err(AppError::Candidate(format!(
            "expected one {package} package archive in {}, found {}",
            directory.display(),
            matches.len()
        )));
    }
    matches
        .pop()
        .ok_or_else(|| AppError::Candidate(format!("lost discovered {package} package archive")))
}

fn build_adapter(manifest: &Path, target: &Path) -> Result<(), AppError> {
    let mut command = Command::new("cargo");
    command
        .arg("build")
        .arg("--manifest-path")
        .arg(manifest)
        .arg("--target-dir")
        .arg(target);
    run(&mut command, "build packaged Kafkars adapter")
}

fn digest(path: &Path) -> Result<String, AppError> {
    let bytes = fs::read(path)
        .map_err(|error| AppError::io(format!("failed to read {}", path.display()), error))?;
    Ok(hex::encode(Sha256::digest(bytes)))
}

fn canonical_directory(path: &Path, label: &str) -> Result<PathBuf, AppError> {
    let path = fs::canonicalize(path)
        .map_err(|error| AppError::io(format!("failed to resolve {}", path.display()), error))?;
    if !path.is_dir() {
        return Err(AppError::Candidate(format!(
            "{label} is not a directory: {}",
            path.display()
        )));
    }
    Ok(path)
}

fn run(command: &mut Command, label: &str) -> Result<(), AppError> {
    command
        .env("LANG", "C")
        .env("LC_ALL", "C")
        .env("LC_CTYPE", "C");
    let status = command
        .status()
        .map_err(|error| AppError::io(format!("failed to {label}"), error))?;
    if status.success() {
        Ok(())
    } else {
        Err(AppError::Candidate(format!("{label} exited with {status}")))
    }
}

fn candidate_time() -> Result<u64, AppError> {
    unix_ms().map_err(|error| AppError::Candidate(error.to_string()))
}
