//! Candidate tests pin archive discovery and content-addressed adapter inputs.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::candidate::find_archive;
use crate::candidate_manifest::{PackageArtifact, adapter_manifest, bundle_digest};

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
fn every_package_digest_contributes_to_bundle_identity() {
    let first = artifacts();
    let mut changed = first.clone();
    changed[0].digest = "f".repeat(64);

    assert_ne!(bundle_digest(&first), bundle_digest(&changed));
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
        source: PathBuf::from(format!("/sources/{name}")),
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
