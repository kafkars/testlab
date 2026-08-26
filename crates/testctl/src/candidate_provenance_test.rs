//! Candidate provenance tests pin source-mode selection and registry checksums.

use std::fmt::Write as _;

use crate::candidate_manifest::PackageSource;
use crate::candidate_provenance::{
    CandidateDependencyMode, PublishedRequirements, dependency_mode_text, published_artifacts_text,
    verify_published_resolution_text,
};

#[test]
fn sibling_paths_select_the_existing_source_package_mode() {
    let manifest = sibling_workspace();

    assert!(matches!(
        must(dependency_mode_text(&manifest), "classify sibling mode"),
        CandidateDependencyMode::SiblingSource
    ));
}

#[test]
fn published_mode_binds_normalized_requirements_and_registry_checksums() {
    let requirements = published_requirements();
    let lock = published_lock('a');
    let artifacts = must(
        published_artifacts_text(&normalized_engine(), &lock, &requirements),
        "bind published artifacts",
    );

    assert_eq!(artifacts.len(), 6);
    assert!(
        artifacts
            .iter()
            .all(|artifact| matches!(&artifact.source, PackageSource::Registry))
    );
    must(
        verify_published_resolution_text(&lock, &artifacts),
        "verify identical adapter resolution",
    );

    let error = verify_published_resolution_text(&published_lock('b'), &artifacts)
        .err()
        .unwrap_or_else(|| panic!("checksum drift unexpectedly passed"));
    assert!(error.to_string().contains("does not match candidate lock"));

    let version_drift = lock.replacen("version = \"0.1.0-rc.2\"", "version = \"0.1.0-rc.9\"", 1);
    let error = verify_published_resolution_text(&version_drift, &artifacts)
        .err()
        .unwrap_or_else(|| panic!("version drift unexpectedly passed"));
    assert!(error.to_string().contains("does not match candidate lock"));
}

#[test]
fn published_mode_rejects_unproven_registry_lock_entries() {
    let requirements = published_requirements();
    let normalized = normalized_engine();
    let lock = published_lock('a');

    let missing = lock.replace(&locked_package("kafka-wire-records", "0.1.0-rc.3", 'a'), "");
    assert_artifact_error(&normalized, &missing, &requirements, "is missing");

    let mut duplicate = lock.clone();
    duplicate.push_str(&locked_package("kafka-driver", "0.1.0-rc.2", 'a'));
    assert_artifact_error(&normalized, &duplicate, &requirements, "multiple versions");

    let wrong_source = lock.replacen(
        "registry+https://github.com/rust-lang/crates.io-index",
        "registry+https://example.invalid/index",
        1,
    );
    assert_artifact_error(&normalized, &wrong_source, &requirements, "from crates.io");

    let malformed = lock.replacen(&"a".repeat(64), "not-a-sha256", 1);
    assert_artifact_error(&normalized, &malformed, &requirements, "valid checksum");
}

#[test]
fn published_mode_rejects_packaged_requirement_or_source_drift() {
    let requirements = published_requirements();
    let version_drift = normalized_engine().replacen("=0.1.0-rc.2", "=0.1.0-rc.9", 1);
    assert_artifact_error(
        &version_drift,
        &published_lock('a'),
        &requirements,
        "does not match candidate",
    );

    let source_drift = normalized_engine().replacen(
        "version = \"=0.1.0-rc.2\"",
        "version = \"=0.1.0-rc.2\"\npath = \"../kafka-driver\"",
        1,
    );
    assert_artifact_error(
        &source_drift,
        &published_lock('a'),
        &requirements,
        "source override",
    );
}

#[test]
fn published_mode_rejects_nonexact_or_mixed_dependencies() {
    let nonexact = dependency_mode_text(&published_workspace("0.1.0-rc.2"))
        .err()
        .unwrap_or_else(|| panic!("nonexact driver requirement unexpectedly passed"));
    assert!(nonexact.to_string().contains("must use an exact version"));

    let partial = dependency_mode_text(&published_workspace("=0.1"))
        .err()
        .unwrap_or_else(|| panic!("partial exact requirement unexpectedly passed"));
    assert!(partial.to_string().contains("must use an exact version"));

    let mixed = published_workspace("=0.1.0-rc.2").replace(
        "kafka-wire = \"=0.1.0-rc.3\"",
        "kafka-wire = { path = \"../kafka-protocol/crates/kafka-wire\" }",
    );
    let error = dependency_mode_text(&mixed)
        .err()
        .unwrap_or_else(|| panic!("mixed dependency sources unexpectedly passed"));
    assert!(error.to_string().contains("mix sibling paths"));

    let wrong_path = sibling_workspace().replace("../kafka-driver", "../unreviewed-driver");
    let error = dependency_mode_text(&wrong_path)
        .err()
        .unwrap_or_else(|| panic!("noncanonical sibling path unexpectedly passed"));
    assert!(error.to_string().contains("noncanonical"));
}

fn sibling_workspace() -> String {
    r#"
[workspace.dependencies]
kafka-driver = { path = "../kafka-driver", version = "0.1.0-rc.2" }
kafka-wire = { path = "../kafka-protocol/crates/kafka-wire", version = "0.1.0-rc.2" }
kafka-wire-core = { path = "../kafka-protocol/crates/kafka-wire-core", version = "0.1.0-rc.2" }
kafka-wire-records = { path = "../kafka-protocol/crates/kafka-wire-records", version = "0.1.0-rc.2" }
"#
    .to_owned()
}

fn published_requirements() -> PublishedRequirements {
    let CandidateDependencyMode::PublishedRegistry(requirements) = must(
        dependency_mode_text(&published_workspace("=0.1.0-rc.2")),
        "classify published mode",
    ) else {
        panic!("published registry mode expected");
    };
    requirements
}

fn assert_artifact_error(
    normalized: &str,
    lock: &str,
    requirements: &PublishedRequirements,
    expected: &str,
) {
    let error = published_artifacts_text(normalized, lock, requirements)
        .err()
        .unwrap_or_else(|| panic!("invalid registry evidence unexpectedly passed"));
    assert!(error.to_string().contains(expected), "{error}");
}

fn published_workspace(driver: &str) -> String {
    format!(
        r#"
[workspace.dependencies]
kafka-driver = "{driver}"
kafka-wire = "=0.1.0-rc.3"
kafka-wire-core = "=0.1.0-rc.3"
kafka-wire-records = "=0.1.0-rc.3"
"#
    )
}

fn normalized_engine() -> String {
    let mut manifest = String::new();
    for (name, version) in [
        ("kafka-driver", "0.1.0-rc.2"),
        ("kafka-wire", "0.1.0-rc.3"),
        ("kafka-wire-core", "0.1.0-rc.3"),
        ("kafka-wire-records", "0.1.0-rc.3"),
    ] {
        write!(
            &mut manifest,
            "[dependencies.{name}]\nversion = \"={version}\"\n"
        )
        .unwrap_or_else(|error| panic!("write normalized manifest fixture: {error}"));
    }
    manifest
}

fn published_lock(checksum: char) -> String {
    let packages = [
        ("kafka-driver", "0.1.0-rc.2"),
        ("kafka-driver-core", "0.1.0-rc.2"),
        ("kafka-driver-transport", "0.1.0-rc.2"),
        ("kafka-wire", "0.1.0-rc.3"),
        ("kafka-wire-core", "0.1.0-rc.3"),
        ("kafka-wire-records", "0.1.0-rc.3"),
    ];
    let mut lock = "version = 4\n".to_owned();
    for (name, version) in packages {
        lock.push_str(&locked_package(name, version, checksum));
    }
    lock
}

fn locked_package(name: &str, version: &str, checksum: char) -> String {
    format!(
        "[[package]]\nname = \"{name}\"\nversion = \"{version}\"\nsource = \"registry+https://github.com/rust-lang/crates.io-index\"\nchecksum = \"{}\"\n",
        checksum.to_string().repeat(64)
    )
}

fn must<T, E: std::fmt::Display>(result: Result<T, E>, context: &str) -> T {
    result.unwrap_or_else(|error| panic!("{context}: {error}"))
}
