//! Qualification runner tests prove fail-closed aggregate sealing.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use sha2::{Digest, Sha256};
use testlab_schema::{QualificationEvidenceManifest, VerdictStatus};

use crate::catalog::Repository;
use crate::qualification::run_qualification;

#[test]
fn missing_subject_qualifies_as_a_complete_invalid_evidence_set() {
    let repository_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let fixture = fixture_directory(&repository_root);
    let _cleanup = Cleanup(fixture.clone());
    must(fs::create_dir_all(&fixture), "create qualification fixture");
    let subject_path = fixture.join("missing-subject.toml");
    must(
        fs::write(
            &subject_path,
            concat!(
                "schema_version = 2\n",
                "id = \"missing-qualification-subject\"\n",
                "display_name = \"missing qualification subject\"\n",
                "artifacts = []\n",
                "command = \"target/definitely-missing-testlab-adapter\"\n",
                "args = []\n",
                "working_directory = \".\"\n",
            ),
        ),
        "write subject fixture",
    );
    let repository = must(Repository::open(&repository_root), "open repository");
    let evidence_root = fixture.join("evidence");

    let run = must(
        run_qualification(
            &repository,
            Path::new("qualifications/repository-pr.toml"),
            &subject_path,
            &evidence_root,
            None,
        ),
        "run invalid qualification",
    );

    assert_eq!(run.status, VerdictStatus::Invalid);
    assert!(run.path.is_dir());
    assert!(!run.path.to_string_lossy().ends_with(".partial"));
    let manifest: QualificationEvidenceManifest = read_json(&run.path.join("manifest.json"));
    assert_eq!(manifest.validate(), Ok(()));
    assert_eq!(manifest.cells.len(), 1);
    assert_eq!(manifest.cells[0].attempts, 1);
    assert_eq!(manifest.cells[0].runs.len(), 3);
    for scenario in &manifest.cells[0].runs {
        assert_eq!(scenario.attempt, 1);
        assert_eq!(scenario.status, VerdictStatus::Invalid);
        assert!(run.path.join(&scenario.evidence_path).is_dir());
    }
    assert_recursive_digests(&run.path);
}

fn assert_recursive_digests(root: &Path) {
    let digests: BTreeMap<String, String> = read_json(&root.join("digests.json"));
    assert!(digests.keys().any(|name| name.starts_with("cells/")));
    for (name, expected) in digests {
        let bytes = must(fs::read(root.join(name)), "read qualification artifact");
        assert_eq!(hex::encode(Sha256::digest(bytes)), expected);
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
        "qualification-test-{}-{}",
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
