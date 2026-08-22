//! Candidate tests pin archive discovery and content-addressed adapter inputs.

use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::candidate::find_archive;
use crate::candidate_manifest::{PackageArtifact, adapter_manifest, bundle_digest};

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
    ["kafka-client-core", "kafka-client-engine", "kafkars"]
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
    let elapsed = must(
        SystemTime::now().duration_since(UNIX_EPOCH),
        "read system time",
    );
    std::env::temp_dir().join(format!(
        "testlab-candidate-test-{}-{}",
        std::process::id(),
        elapsed.as_nanos()
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
