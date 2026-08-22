//! Runner tests prove that post-identity execution failures still seal invalid evidence.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use sha2::{Digest, Sha256};
use testlab_schema::{EvidenceManifest, HistoryEntry, HistoryPayload, Verdict, VerdictStatus};

use crate::catalog::Repository;
use crate::runner::run_scenario;

const SCENARIO: &str = "scenarios/producer/round-trip.toml";

#[test]
fn missing_subject_executable_seals_invalid_evidence() {
    let repository_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let fixture = fixture_directory(&repository_root);
    let _cleanup = Cleanup(fixture.clone());
    must(fs::create_dir_all(&fixture), "create runner fixture");
    let subject_path = fixture.join("missing-subject.toml");
    must(
        fs::write(
            &subject_path,
            concat!(
                "schema_version = 1\n",
                "id = \"missing-subject\"\n",
                "display_name = \"missing subject executable\"\n",
                "command = \"target/definitely-missing-testlab-adapter\"\n",
                "args = []\n",
                "working_directory = \".\"\n",
            ),
        ),
        "write missing-subject manifest",
    );
    let repository = must(Repository::open(&repository_root), "open repository");
    let evidence_root = fixture.join("evidence");
    let sealed = must(
        run_scenario(
            &repository,
            Path::new(SCENARIO),
            &subject_path,
            &evidence_root,
        ),
        "run invalid subject",
    );

    assert_eq!(sealed.verdict.status, VerdictStatus::Invalid);
    assert!(sealed.path.is_dir());
    assert!(!sealed.path.to_string_lossy().ends_with(".partial"));
    assert_required_artifacts(&sealed.path);
    assert_invalid_manifest(&sealed.path);
    assert_harness_failure(&sealed.path);
    assert_digests(&sealed.path);
    assert_no_partial_run(&evidence_root);
}

fn assert_required_artifacts(run: &Path) {
    for name in [
        "manifest.json",
        "scenario.json",
        "subject.json",
        "history.jsonl",
        "broker-observations.jsonl",
        "verdict.json",
        "summary.md",
        "reproduction.sh",
        "digests.json",
    ] {
        assert!(run.join(name).is_file(), "missing evidence artifact {name}");
    }
    assert!(!run.join("adapter.json").exists());
}

fn assert_invalid_manifest(run: &Path) {
    let manifest: EvidenceManifest = read_json(&run.join("manifest.json"));
    let verdict: Verdict = read_json(&run.join("verdict.json"));
    assert_eq!(manifest.status, VerdictStatus::Invalid);
    assert!(manifest.adapter.is_none());
    assert_eq!(verdict.status, VerdictStatus::Invalid);
    assert_eq!(verdict.violations.len(), 1);
    assert_eq!(verdict.violations[0].contract_id.as_str(), "HARNESS-001");
}

fn assert_harness_failure(run: &Path) {
    let source = must(
        fs::read_to_string(run.join("history.jsonl")),
        "read history",
    );
    let mut entries = Vec::new();
    for line in source.lines() {
        entries.push(must(
            serde_json::from_str::<HistoryEntry>(line),
            "parse history",
        ));
    }
    assert!(entries.iter().any(|entry| matches!(
        &entry.payload,
        HistoryPayload::HarnessError { error }
            if error.code == "subject_executable_missing"
    )));
}

fn assert_digests(run: &Path) {
    let digests: BTreeMap<String, String> = read_json(&run.join("digests.json"));
    assert!(!digests.contains_key("digests.json"));
    for (name, expected) in digests {
        let bytes = must(fs::read(run.join(name)), "read digested artifact");
        assert_eq!(hex::encode(Sha256::digest(bytes)), expected);
    }
}

fn assert_no_partial_run(evidence_root: &Path) {
    let entries = must(fs::read_dir(evidence_root), "list evidence root");
    for entry in entries {
        let entry = must(entry, "read evidence entry");
        assert!(!entry.file_name().to_string_lossy().ends_with(".partial"));
    }
}

fn read_json<T: serde::de::DeserializeOwned>(path: &Path) -> T {
    let bytes = must(fs::read(path), "read JSON artifact");
    must(serde_json::from_slice(&bytes), "parse JSON artifact")
}

fn fixture_directory(repository_root: &Path) -> PathBuf {
    let elapsed = must(
        SystemTime::now().duration_since(UNIX_EPOCH),
        "read system time",
    );
    repository_root.join("target").join(format!(
        "runner-test-{}-{}",
        std::process::id(),
        elapsed.as_nanos()
    ))
}

fn must<T, E: std::fmt::Display>(result: Result<T, E>, context: &str) -> T {
    match result {
        Ok(value) => value,
        Err(error) => panic!("{context}: {error}"),
    }
}

struct Cleanup(PathBuf);

impl Drop for Cleanup {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
