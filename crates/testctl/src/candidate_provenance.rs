//! Candidate dependency modes preserve exact source or registry package provenance.

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use serde::Deserialize;

use crate::candidate_manifest::{PackageArtifact, PackageSource, is_complete_semver};
use crate::run_error::AppError;

const CRATES_IO_SOURCE: &str = "registry+https://github.com/rust-lang/crates.io-index";
const DIRECT_SUPPORT: [(&str, &str); 4] = [
    ("kafka-driver", "../kafka-driver"),
    ("kafka-wire", "../kafka-protocol/crates/kafka-wire"),
    (
        "kafka-wire-core",
        "../kafka-protocol/crates/kafka-wire-core",
    ),
    (
        "kafka-wire-records",
        "../kafka-protocol/crates/kafka-wire-records",
    ),
];
const SUPPORT_PACKAGE_NAMES: [&str; 6] = [
    "kafka-driver",
    "kafka-driver-core",
    "kafka-driver-transport",
    "kafka-wire",
    "kafka-wire-core",
    "kafka-wire-records",
];

#[derive(Debug)]
pub(crate) enum CandidateDependencyMode {
    SiblingSource,
    PublishedRegistry(PublishedRequirements),
}

#[derive(Debug)]
pub(crate) struct PublishedRequirements {
    direct: BTreeMap<String, String>,
}

pub(crate) fn dependency_mode(path: &Path) -> Result<CandidateDependencyMode, AppError> {
    let source = read_text(path, "candidate workspace manifest")?;
    dependency_mode_text(&source)
}

pub(crate) fn dependency_mode_text(source: &str) -> Result<CandidateDependencyMode, AppError> {
    let manifest = source
        .parse::<toml::Value>()
        .map_err(|error| candidate(format!("parse candidate workspace manifest: {error}")))?;
    let dependencies = manifest
        .get("workspace")
        .and_then(|value| value.get("dependencies"))
        .and_then(toml::Value::as_table)
        .ok_or_else(|| candidate("candidate manifest has no workspace dependencies"))?;
    let mut paths = 0;
    let mut direct = BTreeMap::new();
    for (name, expected_path) in DIRECT_SUPPORT {
        let dependency = dependencies
            .get(name)
            .ok_or_else(|| candidate(format!("candidate manifest is missing {name}")))?;
        if path_dependency(dependency, name, expected_path)? {
            paths += 1;
        } else {
            direct.insert(name.to_owned(), exact_registry_version(dependency, name)?);
        }
    }
    if paths == DIRECT_SUPPORT.len() {
        return Ok(CandidateDependencyMode::SiblingSource);
    }
    if paths != 0 {
        return Err(candidate(
            "driver and wire dependencies mix sibling paths with registry packages",
        ));
    }
    Ok(CandidateDependencyMode::PublishedRegistry(
        PublishedRequirements { direct },
    ))
}

pub(crate) fn published_artifacts(
    normalized_manifest: &Path,
    lock: &Path,
    requirements: &PublishedRequirements,
) -> Result<Vec<PackageArtifact>, AppError> {
    let normalized = read_text(normalized_manifest, "packaged engine manifest")?;
    let lock = read_text(lock, "candidate Cargo.lock")?;
    published_artifacts_text(&normalized, &lock, requirements)
}

pub(crate) fn published_artifacts_text(
    normalized: &str,
    lock: &str,
    requirements: &PublishedRequirements,
) -> Result<Vec<PackageArtifact>, AppError> {
    verify_normalized_manifest(normalized, requirements)?;
    let lock = parse_lock(lock)?;
    SUPPORT_PACKAGE_NAMES
        .iter()
        .map(|name| {
            let artifact = locked_registry_artifact(&lock, name)?;
            if let Some(expected) = requirements.direct.get(*name)
                && artifact.version != *expected
            {
                return Err(candidate(format!(
                    "packaged {name} requires {expected}, lock selected {}",
                    artifact.version
                )));
            }
            Ok(artifact)
        })
        .collect()
}

pub(crate) fn verify_published_resolution(
    lock: &Path,
    expected: &[PackageArtifact],
) -> Result<(), AppError> {
    let lock = read_text(lock, "candidate adapter Cargo.lock")?;
    verify_published_resolution_text(&lock, expected)
}

pub(crate) fn verify_published_resolution_text(
    lock: &str,
    expected: &[PackageArtifact],
) -> Result<(), AppError> {
    let lock = parse_lock(lock)?;
    for name in SUPPORT_PACKAGE_NAMES {
        let mut matches = expected.iter().filter(|artifact| artifact.name == name);
        let expected = matches
            .next()
            .ok_or_else(|| candidate(format!("missing expected registry artifact {name}")))?;
        if matches.next().is_some() || !matches!(&expected.source, PackageSource::Registry) {
            return Err(candidate(format!(
                "expected registry artifact {name} is noncanonical"
            )));
        }
        let actual = locked_registry_artifact(&lock, name)?;
        if actual.version != expected.version || actual.digest != expected.digest {
            return Err(candidate(format!(
                "resolved registry artifact {name} does not match candidate lock"
            )));
        }
    }
    Ok(())
}

fn verify_normalized_manifest(
    source: &str,
    requirements: &PublishedRequirements,
) -> Result<(), AppError> {
    let manifest = source
        .parse::<toml::Value>()
        .map_err(|error| candidate(format!("parse packaged engine manifest: {error}")))?;
    let dependencies = manifest
        .get("dependencies")
        .and_then(toml::Value::as_table)
        .ok_or_else(|| candidate("packaged engine manifest has no dependencies"))?;
    for (name, _) in DIRECT_SUPPORT {
        let dependency = dependencies
            .get(name)
            .ok_or_else(|| candidate(format!("packaged engine manifest is missing {name}")))?;
        let actual = exact_registry_version(dependency, name)?;
        let expected = requirements
            .direct
            .get(name)
            .ok_or_else(|| candidate(format!("lost candidate requirement for {name}")))?;
        if actual != *expected {
            return Err(candidate(format!(
                "packaged {name} requirement {actual} does not match candidate {expected}"
            )));
        }
    }
    Ok(())
}

fn path_dependency(value: &toml::Value, name: &str, expected_path: &str) -> Result<bool, AppError> {
    let Some(table) = value.as_table() else {
        return Ok(false);
    };
    let Some(path) = table.get("path") else {
        return Ok(false);
    };
    if path.as_str() != Some(expected_path)
        || table.contains_key("git")
        || table.contains_key("registry")
        || table.contains_key("package")
    {
        return Err(candidate(format!(
            "candidate sibling dependency {name} is noncanonical"
        )));
    }
    Ok(true)
}

fn exact_registry_version(value: &toml::Value, name: &str) -> Result<String, AppError> {
    let requirement = if let Some(requirement) = value.as_str() {
        requirement
    } else {
        let table = value
            .as_table()
            .ok_or_else(|| candidate(format!("candidate dependency {name} is malformed")))?;
        if ["path", "git", "registry", "package"]
            .iter()
            .any(|key| table.contains_key(*key))
        {
            return Err(candidate(format!(
                "candidate registry dependency {name} has a source override"
            )));
        }
        table
            .get("version")
            .and_then(toml::Value::as_str)
            .ok_or_else(|| candidate(format!("candidate dependency {name} has no version")))?
    };
    requirement
        .strip_prefix('=')
        .filter(|version| is_complete_semver(version))
        .map(str::to_owned)
        .ok_or_else(|| {
            candidate(format!(
                "candidate dependency {name} must use an exact version"
            ))
        })
}

fn locked_registry_artifact(lock: &CargoLock, name: &str) -> Result<PackageArtifact, AppError> {
    let mut packages = lock.package.iter().filter(|package| package.name == name);
    let package = packages
        .next()
        .ok_or_else(|| candidate(format!("candidate lock is missing {name}")))?;
    if packages.next().is_some() {
        return Err(candidate(format!(
            "candidate lock has multiple versions of {name}"
        )));
    }
    if package.source.as_deref() != Some(CRATES_IO_SOURCE) {
        return Err(candidate(format!(
            "candidate lock does not source {name} from crates.io"
        )));
    }
    let checksum = package
        .checksum
        .as_deref()
        .filter(|checksum| valid_checksum(checksum))
        .ok_or_else(|| candidate(format!("candidate lock has no valid checksum for {name}")))?;
    Ok(PackageArtifact {
        name: name.to_owned(),
        version: package.version.clone(),
        digest: checksum.to_owned(),
        source: PackageSource::Registry,
    })
}

fn parse_lock(source: &str) -> Result<CargoLock, AppError> {
    toml::from_str(source)
        .map_err(|error| candidate(format!("parse candidate Cargo.lock: {error}")))
}

fn read_text(path: &Path, label: &str) -> Result<String, AppError> {
    fs::read_to_string(path)
        .map_err(|error| AppError::io(format!("failed to read {label} {}", path.display()), error))
}

fn valid_checksum(checksum: &str) -> bool {
    checksum.len() == 64
        && checksum
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn candidate(message: impl Into<String>) -> AppError {
    AppError::Candidate(message.into())
}

#[derive(Deserialize)]
struct CargoLock {
    package: Vec<LockedPackage>,
}

#[derive(Deserialize)]
struct LockedPackage {
    name: String,
    version: String,
    source: Option<String>,
    checksum: Option<String>,
}
