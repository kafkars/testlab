//! Compose lifecycle tests use an external fake to prove ordering and cleanup.

use std::fs;
use std::path::PathBuf;
use std::time::Duration;

use testlab_schema::{
    Authentication, EnvironmentManifest, EnvironmentOperationKind, EnvironmentOperationStatus,
    RunId, SecurityProfile, TransportSecurity,
};

use crate::compose_test_fixture::Fixture;
use crate::{ComposeRequest, DockerComposeEnvironment};

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

#[test]
fn scram_setup_is_correlated_without_recording_the_password() {
    let fixture = Fixture::with_authentication(false, Authentication::ScramSha256);
    let mut environment = fixture.environment();

    let setup = environment.start(Duration::from_secs(2));
    let _cleanup = environment.finish(Duration::from_secs(2));

    let operation = setup
        .operations
        .iter()
        .find(|operation| operation.kind == EnvironmentOperationKind::BrokerSecuritySetup)
        .unwrap_or_else(|| panic!("missing broker security setup operation"));
    assert!(
        !operation
            .args
            .join(" ")
            .contains("kafkars-testlab-password")
    );
    assert!(operation.args.join(" ").contains("$TESTLAB_SCRAM_PASSWORD"));
}

#[test]
fn tls_ca_copy_is_a_correlated_security_operation() {
    let fixture = Fixture::with_security(
        false,
        SecurityProfile {
            transport: TransportSecurity::TlsCustom,
            authentication: Authentication::None,
        },
    );
    let mut environment = fixture.environment();

    let setup = environment.start(Duration::from_secs(2));
    let _cleanup = environment.finish(Duration::from_secs(2));

    let operation = setup
        .operations
        .iter()
        .find(|operation| operation.kind == EnvironmentOperationKind::BrokerSecuritySetup)
        .unwrap_or_else(|| panic!("missing TLS CA copy operation"));
    let args = operation.args.join(" ");
    assert!(args.contains("cp broker:/etc/kafka/secrets/ca.pem"));
    assert!(args.contains("target/testlab-security/run-compose-"));
    assert!(!args.contains("ca.key"));
}

#[test]
fn feature_setup_follows_readiness_and_precedes_client_security() {
    let fixture = Fixture::with_feature_level("share.version", 1);
    let mut environment = fixture.environment();

    let setup = environment.start(Duration::from_secs(2));
    let _cleanup = environment.finish(Duration::from_secs(2));

    let feature = setup
        .operations
        .iter()
        .position(|operation| operation.kind == EnvironmentOperationKind::BrokerFeatureSetup)
        .unwrap_or_else(|| panic!("missing broker feature setup operation"));
    let last_readiness = setup
        .operations
        .iter()
        .rposition(|operation| operation.kind == EnvironmentOperationKind::Readiness)
        .unwrap_or_else(|| panic!("missing readiness operation"));
    assert!(last_readiness < feature);
    let security = setup
        .operations
        .iter()
        .position(|operation| operation.kind == EnvironmentOperationKind::BrokerSecuritySetup)
        .unwrap_or_else(|| panic!("missing broker security setup operation"));
    assert!(feature < security);
    assert_eq!(
        setup.operations[feature].args,
        [
            "compose",
            "--project-name",
            setup.operations[feature].args[2].as_str(),
            "--file",
            "clusters/kafka.yml",
            "exec",
            "--no-TTY",
            "broker",
            "/opt/kafka/bin/kafka-features.sh",
            "--bootstrap-server",
            "localhost:9092",
            "upgrade",
            "--feature",
            "share.version=1",
        ]
    );
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
