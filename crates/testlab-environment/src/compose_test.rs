//! Compose lifecycle tests use an external fake to prove ordering and cleanup.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use testlab_schema::{
    Authentication, BrokerIdentity, EnvironmentDriver, EnvironmentId, EnvironmentManifest,
    EnvironmentOperationKind, EnvironmentOperationStatus, RunId, SecurityProfile,
    TransportSecurity,
};

use crate::{ComposeRequest, DockerComposeEnvironment};

static FIXTURE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[test]
#[ignore = "requires the local Docker engine and pinned Kafka image"]
fn checked_in_kafka_starts_becomes_ready_and_cleans_up() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let source = fs::read_to_string(root.join("clusters/apache-kafka/4.3.1/single-plaintext.toml"))
        .unwrap_or_else(|error| panic!("read checked environment: {error}"));
    let manifest: EnvironmentManifest = toml::from_str(&source)
        .unwrap_or_else(|error| panic!("parse checked environment: {error}"));
    let run_id = RunId::new(format!("run-docker-{}", std::process::id()))
        .unwrap_or_else(|error| panic!("Docker run id: {error}"));
    let mut environment = DockerComposeEnvironment::new(ComposeRequest {
        repository_root: &root,
        environment: &manifest,
        run_id: &run_id,
        started_unix_ms: 1,
    })
    .unwrap_or_else(|error| panic!("create Docker environment: {error}"));

    let setup = environment.start(Duration::from_secs(60));
    let cleanup = environment.finish(Duration::from_secs(20));

    assert!(setup.succeeded(), "setup failure: {:?}", setup.failure);
    assert!(
        cleanup.succeeded(),
        "cleanup failure: {:?}",
        cleanup.failure
    );
    assert!(
        setup.artifacts.iter().any(|artifact| {
            artifact.name.starts_with("readiness-") && !artifact.bytes.is_empty()
        })
    );
}

#[test]
fn lifecycle_retries_readiness_and_retains_cleanup_evidence() {
    let fixture = Fixture::new(false);
    let mut environment = fixture.environment();

    let setup = environment.start(Duration::from_secs(2));

    assert!(setup.succeeded(), "setup failure: {:?}", setup.failure);
    assert_eq!(setup.operations.len(), 6);
    assert_eq!(
        setup.operations[4].status,
        EnvironmentOperationStatus::Failed
    );
    assert_eq!(
        setup.operations[5].status,
        EnvironmentOperationStatus::Succeeded
    );
    assert_unique_operation_ids(&setup.operations);
    assert_eq!(environment.endpoint(), "127.0.0.1:29092");

    let cleanup = environment.finish(Duration::from_secs(2));

    assert!(
        cleanup.succeeded(),
        "cleanup failure: {:?}",
        cleanup.failure
    );
    assert_eq!(
        cleanup
            .operations
            .iter()
            .map(|operation| operation.kind)
            .collect::<Vec<_>>(),
        vec![
            EnvironmentOperationKind::ComposePs,
            EnvironmentOperationKind::ComposeLogs,
            EnvironmentOperationKind::ComposeDown,
        ]
    );
    let log = fixture.log();
    assert!(log.contains("pull apache/kafka@sha256:"));
    assert!(log.contains("image inspect apache/kafka@sha256:"));
    assert!(log.contains("down --volumes --remove-orphans"));
}

#[test]
fn failed_up_attempt_still_runs_cleanup() {
    let fixture = Fixture::new(true);
    let mut environment = fixture.environment();

    let setup = environment.start(Duration::from_secs(2));
    let cleanup = environment.finish(Duration::from_secs(2));

    assert_eq!(
        setup.failure.as_ref().map(crate::ComposeFailure::code),
        Some("environment_compose_up_failed")
    );
    assert!(
        cleanup
            .operations
            .iter()
            .any(|operation| operation.kind == EnvironmentOperationKind::ComposeDown)
    );
    assert!(fixture.log().contains("down --volumes --remove-orphans"));
}

fn assert_unique_operation_ids(operations: &[testlab_schema::EnvironmentOperation]) {
    for (index, operation) in operations.iter().enumerate() {
        assert!(
            operations[..index]
                .iter()
                .all(|prior| prior.id != operation.id)
        );
    }
}

struct Fixture {
    root: PathBuf,
    program: PathBuf,
    manifest: EnvironmentManifest,
    run_id: RunId,
}

impl Fixture {
    fn new(fail_up: bool) -> Self {
        let sequence = FIXTURE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let root =
            std::env::temp_dir().join(format!("testlab-compose-{}-{sequence}", std::process::id()));
        fs::create_dir(&root)
            .unwrap_or_else(|error| panic!("create fixture {}: {error}", root.display()));
        let program = root.join("fake-docker");
        fs::write(&program, fake_docker(fail_up))
            .unwrap_or_else(|error| panic!("write fake Docker program: {error}"));
        make_executable(&program);
        Self {
            root,
            program,
            manifest: manifest(),
            run_id: RunId::new(format!("run-compose-{sequence}"))
                .unwrap_or_else(|error| panic!("fixture run id: {error}")),
        }
    }

    fn environment(&self) -> DockerComposeEnvironment {
        DockerComposeEnvironment::new_with_program(
            ComposeRequest {
                repository_root: &self.root,
                environment: &self.manifest,
                run_id: &self.run_id,
                started_unix_ms: 1,
            },
            self.program.clone(),
            29092,
        )
        .unwrap_or_else(|error| panic!("create Compose environment: {error}"))
    }

    fn log(&self) -> String {
        fs::read_to_string(self.program.with_extension("log"))
            .unwrap_or_else(|error| panic!("read fake Docker log: {error}"))
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

fn manifest() -> EnvironmentManifest {
    EnvironmentManifest {
        schema_version: 1,
        id: EnvironmentId::new("apache-kafka-test")
            .unwrap_or_else(|error| panic!("fixture environment id: {error}")),
        title: "Apache Kafka test fixture".to_owned(),
        driver: EnvironmentDriver::DockerCompose {
            broker: BrokerIdentity {
                implementation: "apache-kafka".to_owned(),
                version: "4.3.1".to_owned(),
            },
            image: format!("apache/kafka@sha256:{}", "a".repeat(64)),
            cluster_size: 1,
            security: SecurityProfile {
                transport: TransportSecurity::Plaintext,
                authentication: Authentication::None,
            },
            compose_files: vec!["clusters/kafka.yml".to_owned()],
            broker_services: vec!["broker".to_owned()],
            client_port: 9092,
        },
    }
}

fn fake_docker(fail_up: bool) -> String {
    format!(
        "#!/bin/sh\nlog=\"$0.log\"\nprintf '%s\\n' \"$*\" >> \"$log\"\necho \"stdout:$*\"\necho \"stderr:$*\" >&2\ncase \" $* \" in\n  *\" up \"*) if {fail_up}; then exit 9; fi ;;\n  *\"kafka-broker-api-versions.sh\"*)\n    ready=\"$0.ready\"\n    if [ ! -e \"$ready\" ]; then : > \"$ready\"; exit 1; fi ;;\nesac\nexit 0\n"
    )
}

#[cfg(unix)]
fn make_executable(path: &Path) {
    use std::os::unix::fs::PermissionsExt;
    let mut permissions = fs::metadata(path)
        .unwrap_or_else(|error| panic!("inspect fake Docker program: {error}"))
        .permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(path, permissions)
        .unwrap_or_else(|error| panic!("make fake Docker program executable: {error}"));
}

#[cfg(not(unix))]
fn make_executable(_path: &Path) {}
