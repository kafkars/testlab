//! Shard tests exercise selection, complete aggregation, and fail-closed artifact validation.

use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::Value;
use testlab_schema::{
    QualificationEvidenceManifest, SubjectArtifact, SubjectManifest, VerdictStatus,
};

use crate::catalog::Repository;
use crate::evidence_io::digest_tree;
use crate::qualification::{run_qualification, select_qualification};
use crate::qualification_merge::aggregate_qualification;
use crate::qualification_shard::{read_json, same_candidate, verify_shard};

const QUALIFICATION: &str = "qualifications/repository-pr.toml";

#[test]
fn pr_is_one_pass_and_release_retains_repetitions() {
    let repository = repository();
    let (_, pr) = must(repository.load_qualification(Path::new("qualifications/kafkars-pr.toml")));
    let (_, release) =
        must(repository.load_qualification(Path::new("qualifications/kafkars-release.toml")));
    assert_eq!(pr.cells[0].attempts, 1);
    assert_eq!(release.cells.len(), 12);
    assert_eq!(release.cells[0].attempts, 3);
    assert!(release.cells.iter().all(|cell| cell.gating));
}

#[test]
fn selection_has_distinct_identity_and_rejects_unknown_cells() {
    let repository = repository();
    let (_, qualification) = must(repository.load_qualification(Path::new(QUALIFICATION)));
    let selected = must(select_qualification(
        &qualification,
        Some(qualification.cells[0].id.as_str()),
    ));
    assert_ne!(selected.id, qualification.id);
    assert_eq!(selected.cells, qualification.cells);
    assert_eq!(
        must(select_qualification(&qualification, None)),
        qualification
    );
    assert!(select_qualification(&qualification, Some("missing-cell")).is_err());
}

#[test]
fn complete_invalid_shard_seals_complete_invalid_aggregate() {
    let fixture = Fixture::new();
    let result = must(fixture.merge(std::slice::from_ref(&fixture.shard)));
    assert_eq!(result.status, VerdictStatus::Invalid);
    let manifest: QualificationEvidenceManifest =
        must(read_json(&result.path.join("manifest.json")));
    assert_eq!(manifest.qualification_id, fixture.qualification().id);
    assert_eq!(manifest.cells[0].runs.len(), 3);
    let recorded: std::collections::BTreeMap<String, String> =
        must(read_json(&result.path.join("digests.json")));
    assert_eq!(recorded, must(digest_tree(&result.path)));
    let reproduction = must(fs::read_to_string(result.path.join("reproduction.sh")));
    assert!(reproduction.contains("aggregate-qualification"));
}

#[test]
fn missing_extra_and_duplicate_cell_shards_are_rejected() {
    let fixture = Fixture::new();
    assert!(fixture.merge(&[]).is_err());
    assert!(
        fixture
            .merge(&[fixture.shard.clone(), fixture.shard.clone()])
            .is_err()
    );
    let mut qualification = fixture.qualification();
    let mut second = qualification.cells[0].clone();
    second.id = must(testlab_schema::CellId::new("second"));
    second.pack = "packs/kafkars-pr.toml".to_owned();
    qualification.cells.push(second);
    let path = fixture.root.join("two-cells.toml");
    must(fs::write(&path, must(toml::to_string(&qualification))));
    let error = aggregate_qualification(
        &fixture.repository,
        &path,
        &[fixture.shard.clone(), fixture.shard.clone()],
        &fixture.root.join("merged"),
    );
    assert!(error.is_err());
}

#[test]
fn corrupted_or_partial_evidence_is_rejected() {
    let fixture = Fixture::new();
    must(fs::write(
        fixture.shard.join("summary.md"),
        "changed after sealing",
    ));
    assert!(fixture.verify().is_err());
    let partial = fixture.shard.with_extension("partial");
    must(fs::rename(&fixture.shard, &partial));
    assert!(verify_shard(&fixture.repository, &fixture.qualification(), &partial).is_err());
}

#[test]
fn missing_scenario_repetition_and_changed_catalog_are_rejected() {
    for mutation in [
        "missing",
        "attempt",
        "scenario",
        "environment",
        "qualification",
        "verdict",
    ] {
        let fixture = Fixture::new();
        let manifest: QualificationEvidenceManifest =
            must(read_json(&fixture.shard.join("manifest.json")));
        let scenario_root = &manifest.cells[0].runs[0].evidence_path;
        match mutation {
            "missing" => fixture.mutate("manifest.json", |value| {
                value["cells"][0]["runs"]
                    .as_array_mut()
                    .unwrap_or_else(|| panic!("runs"))
                    .pop();
            }),
            "attempt" => fixture.mutate("manifest.json", |value| {
                value["cells"][0]["runs"][0]["attempt"] = 2.into()
            }),
            "scenario" => fixture.mutate(&format!("{scenario_root}/scenario.json"), |value| {
                value["title"] = "changed scenario".into()
            }),
            "environment" => fixture
                .mutate(&format!("{scenario_root}/environment.json"), |value| {
                    value["title"] = "changed environment".into()
                }),
            "qualification" => fixture.mutate("qualification.json", |value| {
                value["cells"][0]["attempts"] = 2.into()
            }),
            "verdict" => fixture.mutate(&format!("{scenario_root}/verdict.json"), |value| {
                value["status"] = "passed".into()
            }),
            _ => panic!("unknown mutation"),
        }
        fixture.redigest();
        assert!(fixture.verify().is_err(), "accepted {mutation}");
    }
}

#[test]
fn candidate_comparison_allows_runner_paths_but_requires_exact_packages() {
    let fixture = Fixture::new();
    let mut first: SubjectManifest = must(read_json(&fixture.shard.join("subject.json")));
    first.artifacts.push(SubjectArtifact {
        name: "kafkars".to_owned(),
        version: "0.0.2-rc.2".to_owned(),
        sha256: "a".repeat(64),
    });
    let mut second = first.clone();
    second.command = "target/another-runner/adapter".to_owned();
    assert!(same_candidate(&first, &second));
    second.artifacts[0].sha256 = "b".repeat(64);
    assert!(!same_candidate(&first, &second));
    second.artifacts.clear();
    assert!(!same_candidate(&first, &second));
}

#[test]
fn failed_scenarios_remain_failed_after_aggregation() {
    let fixture = Fixture::new();
    let manifest: QualificationEvidenceManifest =
        must(read_json(&fixture.shard.join("manifest.json")));
    for run in &manifest.cells[0].runs {
        for name in ["manifest.json", "verdict.json"] {
            fixture.mutate(&format!("{}/{name}", run.evidence_path), |value| {
                value["status"] = "failed".into()
            });
        }
    }
    fixture.mutate("manifest.json", |value| {
        value["status"] = "failed".into();
        value["cells"][0]["status"] = "failed".into();
        for run in value["cells"][0]["runs"]
            .as_array_mut()
            .unwrap_or_else(|| panic!("runs"))
        {
            run["status"] = "failed".into();
        }
    });
    fixture.redigest();
    let result = must(fixture.merge(std::slice::from_ref(&fixture.shard)));
    assert_eq!(result.status, VerdictStatus::Failed);
}

#[cfg(unix)]
#[test]
fn symlinked_artifacts_are_rejected() {
    let fixture = Fixture::new();
    must(std::os::unix::fs::symlink(
        "summary.md",
        fixture.shard.join("linked.md"),
    ));
    assert!(fixture.verify().is_err());
}

struct Fixture {
    repository: Repository,
    root: PathBuf,
    shard: PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let repository = repository();
        let nonce = must(SystemTime::now().duration_since(UNIX_EPOCH)).as_nanos();
        let root = repository
            .root()
            .join("target")
            .join(format!("shard-test-{}-{nonce}", std::process::id()));
        must(fs::create_dir_all(&root));
        let subject = root.join("subject.toml");
        must(fs::write(
            &subject,
            concat!(
                "schema_version = 2\nid = \"missing-shard-subject\"\ndisplay_name = \"missing subject\"\n",
                "command = \"target/definitely-missing-shard-adapter\"\nworking_directory = \".\"\n"
            ),
        ));
        let (_, qualification) = must(repository.load_qualification(Path::new(QUALIFICATION)));
        let run = must(run_qualification(
            &repository,
            Path::new(QUALIFICATION),
            &subject,
            &root.join("shards"),
            Some(qualification.cells[0].id.as_str()),
        ));
        Self {
            repository,
            root,
            shard: run.path,
        }
    }

    fn qualification(&self) -> testlab_schema::QualificationManifest {
        must(self.repository.load_qualification(Path::new(QUALIFICATION))).1
    }

    fn merge(
        &self,
        shards: &[PathBuf],
    ) -> Result<crate::qualification::QualificationRun, crate::run_error::AppError> {
        aggregate_qualification(
            &self.repository,
            Path::new(QUALIFICATION),
            shards,
            &self.root.join("merged"),
        )
    }

    fn verify(
        &self,
    ) -> Result<crate::qualification_shard::VerifiedShard, crate::run_error::AppError> {
        verify_shard(&self.repository, &self.qualification(), &self.shard)
    }

    fn mutate(&self, relative: &str, change: impl FnOnce(&mut Value)) {
        let path = self.shard.join(relative);
        let mut value: Value = must(read_json(&path));
        change(&mut value);
        must(fs::write(path, must(serde_json::to_vec_pretty(&value))));
    }

    fn redigest(&self) {
        let digests = must(digest_tree(&self.shard));
        must(fs::write(
            self.shard.join("digests.json"),
            must(serde_json::to_vec_pretty(&digests)),
        ));
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

fn repository() -> Repository {
    must(Repository::open(
        &Path::new(env!("CARGO_MANIFEST_DIR")).join("../.."),
    ))
}

fn must<T, E: std::fmt::Display>(result: Result<T, E>) -> T {
    match result {
        Ok(value) => value,
        Err(error) => panic!("{error}"),
    }
}
