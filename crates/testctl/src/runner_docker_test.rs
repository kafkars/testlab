//! Docker runner tests prove lifecycle failures still produce complete evidence.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use sha2::{Digest, Sha256};
use testlab_schema::{
    EnvironmentOperationKind, EnvironmentOperationStatus, HistoryEntry, HistoryPayload,
    VerdictStatus,
};

use crate::catalog::Repository;
use crate::runner::run_scenario;

const ENVIRONMENT: &str = "clusters/apache-kafka/4.3.1/single-plaintext.toml";

#[test]
#[ignore = "requires the local Docker engine and pinned Kafka image"]
fn subject_failure_after_kafka_start_seals_cleanup_evidence() {
    let repository_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let fixture = fixture_directory(&repository_root);
    let _cleanup = Cleanup(fixture.clone());
    must(fs::create_dir_all(&fixture), "create Docker runner fixture");
    let scenario_path = fixture.join("real-kafka-timeout.toml");
    let source = must(
        fs::read_to_string(repository_root.join("scenarios/producer/round-trip.toml")),
        "read scenario fixture",
    );
    must(
        fs::write(
            &scenario_path,
            source.replace("timeout_ms = 5000", "timeout_ms = 60000"),
        ),
        "write scenario fixture",
    );
    let subject_path = fixture.join("missing-subject.toml");
    must(
        fs::write(
            &subject_path,
            concat!(
                "schema_version = 2\n",
                "id = \"missing-docker-subject\"\n",
                "display_name = \"missing Docker subject executable\"\n",
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

    let sealed = must(
        run_scenario(
            &repository,
            &scenario_path,
            &subject_path,
            Path::new(ENVIRONMENT),
            &evidence_root,
        ),
        "run Docker-backed invalid subject",
    );

    assert_eq!(sealed.verdict.status, VerdictStatus::Invalid);
    assert_environment_artifacts(&sealed.path);
    assert_lifecycle_history(&sealed.path);
    assert_digests(&sealed.path);
}

fn assert_environment_artifacts(run: &Path) {
    for name in [
        "image-pull.txt",
        "image-inspect.json",
        "compose-config.yml",
        "compose-up.txt",
        "compose-ps.json",
        "broker.log",
        "compose-down.txt",
    ] {
        assert!(
            run.join(name).is_file(),
            "missing environment artifact {name}"
        );
    }
    let readiness = must(fs::read_dir(run), "list sealed evidence").any(|entry| {
        must(entry, "read evidence entry")
            .file_name()
            .to_string_lossy()
            .starts_with("readiness-broker-")
    });
    assert!(readiness, "missing readiness evidence");
}

fn assert_lifecycle_history(run: &Path) {
    let source = must(
        fs::read_to_string(run.join("history.jsonl")),
        "read history",
    );
    let operations = source
        .lines()
        .filter_map(|line| {
            let entry: HistoryEntry = must(serde_json::from_str(line), "parse history");
            match entry.payload {
                HistoryPayload::EnvironmentOperation { operation } => Some(operation),
                _ => None,
            }
        })
        .collect::<Vec<_>>();
    for required in [
        EnvironmentOperationKind::ImagePull,
        EnvironmentOperationKind::ImageInspect,
        EnvironmentOperationKind::ComposeConfig,
        EnvironmentOperationKind::ComposeUp,
        EnvironmentOperationKind::Readiness,
        EnvironmentOperationKind::BrokerProvision,
        EnvironmentOperationKind::ComposePs,
        EnvironmentOperationKind::ComposeLogs,
        EnvironmentOperationKind::ComposeDown,
    ] {
        assert!(
            operations
                .iter()
                .any(|operation| operation.kind == required),
            "missing operation {required:?}"
        );
    }
    assert!(operations.iter().any(|operation| {
        operation.kind == EnvironmentOperationKind::ComposeDown
            && operation.status == EnvironmentOperationStatus::Succeeded
    }));
}

fn assert_digests(run: &Path) {
    let source = must(fs::read(run.join("digests.json")), "read digests");
    let digests: BTreeMap<String, String> = must(serde_json::from_slice(&source), "parse digests");
    for (name, expected) in digests {
        let bytes = must(fs::read(run.join(name)), "read artifact");
        assert_eq!(hex::encode(Sha256::digest(bytes)), expected);
    }
}

fn fixture_directory(repository_root: &Path) -> PathBuf {
    let elapsed = must(
        SystemTime::now().duration_since(UNIX_EPOCH),
        "read system time",
    );
    repository_root.join("target").join(format!(
        "docker-runner-test-{}-{}",
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
