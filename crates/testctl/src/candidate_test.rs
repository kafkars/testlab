//! Candidate tests pin archive discovery and content-addressed adapter inputs.

use std::ffi::OsStr;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::candidate::{build_command, find_archive, kafkars_package_command};
use crate::candidate_manifest::{PackageArtifact, PackageSource, adapter_manifest, bundle_digest};

static FIXTURE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[test]
fn one_exact_package_archive_is_discovered() {
    let fixture = fixture_directory();
    let _cleanup = Cleanup(fixture.clone());
    must(fs::create_dir_all(&fixture), "create candidate fixture");
    must(
        fs::write(fixture.join("kafkars-0.2.0.crate"), b"package"),
        "write package archive",
    );

    let (path, version) = must(find_archive(&fixture, "kafkars"), "find archive");

    assert_eq!(path, fixture.join("kafkars-0.2.0.crate"));
    assert_eq!(version, "0.2.0");
}

#[test]
fn similarly_prefixed_packages_do_not_make_archive_identity_ambiguous() {
    let fixture = fixture_directory();
    let _cleanup = Cleanup(fixture.clone());
    must(fs::create_dir_all(&fixture), "create candidate fixture");
    must(
        fs::write(fixture.join("kafka-driver-0.1.0.crate"), b"driver"),
        "write driver archive",
    );
    must(
        fs::write(
            fixture.join("kafka-driver-core-0.1.0.crate"),
            b"driver core",
        ),
        "write driver core archive",
    );

    let (path, version) = must(
        find_archive(&fixture, "kafka-driver"),
        "find exact driver archive",
    );

    assert_eq!(path, fixture.join("kafka-driver-0.1.0.crate"));
    assert_eq!(version, "0.1.0");
}

#[test]
fn ambiguous_package_archives_are_rejected() {
    let fixture = fixture_directory();
    let _cleanup = Cleanup(fixture.clone());
    must(fs::create_dir_all(&fixture), "create candidate fixture");
    must(
        fs::write(fixture.join("kafkars-0.1.0.crate"), b"one"),
        "write first archive",
    );
    must(
        fs::write(fixture.join("kafkars-0.2.0.crate"), b"two"),
        "write second archive",
    );

    let error = find_archive(&fixture, "kafkars")
        .err()
        .unwrap_or_else(|| panic!("ambiguous archives unexpectedly passed"));

    assert!(error.to_string().contains("found 2"));
}

#[test]
fn adapter_manifest_uses_every_extracted_package() {
    let artifacts = artifacts();

    let manifest = must(
        adapter_manifest(Path::new("/testlab root"), &artifacts),
        "render adapter manifest",
    );

    assert!(manifest.contains("/sources/kafkars"));
    assert!(manifest.contains("/sources/kafka-client-core"));
    assert!(manifest.contains("/sources/kafka-client-engine"));
    assert!(manifest.contains("/sources/kafka-driver"));
    assert!(manifest.contains("/sources/kafka-driver-core"));
    assert!(manifest.contains("/sources/kafka-driver-transport"));
    assert!(manifest.contains("/sources/kafka-wire"));
    assert!(manifest.contains("/sources/kafka-wire-core"));
    assert!(manifest.contains("/sources/kafka-wire-records"));
    assert!(manifest.contains("/testlab root/adapters/kafkars-rust/src/lib.rs"));
}

#[test]
fn adapter_manifest_pins_registry_support_without_patches_or_extra_features() {
    let mut artifacts = artifacts();
    for artifact in &mut artifacts[3..] {
        artifact.source = PackageSource::Registry;
    }

    let manifest = must(
        adapter_manifest(Path::new("/testlab root"), &artifacts),
        "render registry adapter manifest",
    );

    assert!(manifest.contains("kafka-client-engine = { path"));
    assert!(!manifest.contains("kafka-driver = { path"));
    assert!(!manifest.contains("kafka-wire-records = { path"));
    let manifest = must(manifest.parse::<toml::Value>(), "parse registry manifest");
    for artifact in &artifacts[3..] {
        let dependency = &manifest["dependencies"][&artifact.name];
        assert_eq!(
            dependency["version"].as_str(),
            Some(format!("={}", artifact.version).as_str())
        );
        assert_eq!(dependency["default-features"].as_bool(), Some(false));
        assert!(dependency.get("path").is_none());
        assert!(dependency.get("features").is_none());
        assert!(manifest["patch"]["crates-io"].get(&artifact.name).is_none());
    }
}

#[test]
fn registry_constraints_reject_noncanonical_versions() {
    let mut artifacts = artifacts();
    for artifact in &mut artifacts[3..] {
        artifact.source = PackageSource::Registry;
    }
    artifacts[3].version = "0.1".to_owned();
    assert!(adapter_manifest(Path::new("/testlab root"), &artifacts).is_err());
}

#[test]
fn every_package_digest_contributes_to_bundle_identity() {
    let first = artifacts();
    let mut changed = first.clone();
    changed[0].digest = "f".repeat(64);

    assert_ne!(bundle_digest(&first), bundle_digest(&changed));
}

#[test]
fn candidate_packaging_uses_the_reviewed_workspace_overlay_toolchain() {
    let command = kafkars_package_command();

    assert_eq!(command.get_program(), OsStr::new("rustup"));
    assert_eq!(
        command.get_args().collect::<Vec<_>>(),
        ["run", "1.90.0", "cargo"].map(OsStr::new)
    );
    assert!(
        include_str!("../../../action.yml")
            .contains("rustup toolchain install 1.90.0 --profile minimal")
    );
}

#[test]
fn candidate_adapter_build_uses_the_generated_lock() {
    let command = build_command(
        Path::new("/candidate/Cargo.toml"),
        Path::new("/adapter-target"),
    );

    assert_eq!(command.get_program(), OsStr::new("cargo"));
    assert_eq!(
        command.get_args().collect::<Vec<_>>(),
        [
            "build",
            "--manifest-path",
            "/candidate/Cargo.toml",
            "--locked",
            "--target-dir",
            "/adapter-target",
        ]
        .map(OsStr::new)
    );
}

fn artifacts() -> Vec<PackageArtifact> {
    [
        "kafka-client-core",
        "kafka-client-engine",
        "kafkars",
        "kafka-driver",
        "kafka-driver-core",
        "kafka-driver-transport",
        "kafka-wire",
        "kafka-wire-core",
        "kafka-wire-records",
    ]
    .into_iter()
    .map(|name| PackageArtifact {
        name: name.to_owned(),
        version: "0.1.0".to_owned(),
        digest: "a".repeat(64),
        source: PackageSource::Extracted(PathBuf::from(format!("/sources/{name}"))),
    })
    .collect()
}

fn fixture_directory() -> PathBuf {
    let sequence = FIXTURE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "testlab-candidate-test-{}-{}",
        std::process::id(),
        sequence
    ))
}

struct Cleanup(PathBuf);

impl Drop for Cleanup {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn must<T, E: std::fmt::Display>(result: Result<T, E>, context: &str) -> T {
    result.unwrap_or_else(|error| panic!("{context}: {error}"))
}
