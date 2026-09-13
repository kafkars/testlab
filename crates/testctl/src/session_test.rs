//! Session tests preserve cleanup and packaged adapter identity.

use std::collections::{BTreeMap, BTreeSet};

use testlab_schema::{
    AdapterCommand, AdapterDescriptor, AdapterId, ClientId, CreatePartitionsAction,
    CreateTopicAction, OperationId, ScenarioAction, SubjectArtifact, SubjectId, SubjectManifest,
    TOPIC_ALREADY_EXISTS_ERROR_CODE, UNKNOWN_TOPIC_OR_PARTITION_ERROR_CODE,
};

use super::runner_protocol::ExpectedEvent;
use super::session::{expects_admin_failure, scenario_failure_settlement, verify_subject_version};

#[test]
fn scenario_failure_aborts_instead_of_claiming_clean_finish() {
    let (command, expected) = scenario_failure_settlement();

    assert_eq!(command, AdapterCommand::Abort);
    assert!(matches!(expected, ExpectedEvent::Aborted));
}

#[test]
fn only_declared_admin_errors_observe_after_public_failure() {
    let mut action = CreateTopicAction {
        client_id: ClientId::new("client-1").unwrap_or_else(|error| panic!("client: {error}")),
        operation_id: OperationId::new("duplicate-topic")
            .unwrap_or_else(|error| panic!("operation: {error}")),
        topic: "orders".to_owned(),
        partitions: 1,
        replication_factor: 1,
        replica_assignments: None,
        validate_only: false,
        expected_error_code: None,
        timeout_ms: 1_000,
    };

    assert!(!expects_admin_failure(&ScenarioAction::CreateTopic(
        action.clone()
    )));
    action.expected_error_code = Some(TOPIC_ALREADY_EXISTS_ERROR_CODE.to_owned());
    assert!(expects_admin_failure(&ScenarioAction::CreateTopic(action)));

    assert!(expects_admin_failure(&ScenarioAction::CreatePartitions(
        CreatePartitionsAction {
            client_id: ClientId::new("client-1").unwrap_or_else(|error| panic!("client: {error}")),
            operation_id: OperationId::new("missing-topic")
                .unwrap_or_else(|error| panic!("operation: {error}")),
            topic: "missing-orders".to_owned(),
            total_count: 2,
            validate_only: false,
            expected_current_count: None,
            expected_error_code: Some(UNKNOWN_TOPIC_OR_PARTITION_ERROR_CODE.to_owned()),
            timeout_ms: 1_000,
        }
    )));
}

#[test]
fn packaged_kafkars_version_must_match_the_adapter_descriptor() {
    let subject = subject_with_versions(&["0.0.2-rc.2"]);
    let mut descriptor = descriptor("0.0.2-rc.1");

    let error = verify_subject_version(&subject, &descriptor)
        .err()
        .unwrap_or_else(|| panic!("version mismatch should fail"));
    assert!(error.to_string().contains("0.0.2-rc.1"));
    assert!(error.to_string().contains("0.0.2-rc.2"));

    descriptor.version = "0.0.2-rc.2".to_owned();
    assert!(verify_subject_version(&subject, &descriptor).is_ok());
    assert!(verify_subject_version(&subject_with_versions(&[]), &descriptor).is_ok());
}

#[test]
fn multiple_packaged_kafkars_versions_are_ambiguous() {
    let subject = subject_with_versions(&["0.0.2-rc.1", "0.0.2-rc.2"]);

    let error = verify_subject_version(&subject, &descriptor("0.0.2-rc.2"))
        .err()
        .unwrap_or_else(|| panic!("ambiguous versions should fail"));

    assert!(error.to_string().contains("more than one"));
}

fn subject_with_versions(versions: &[&str]) -> SubjectManifest {
    SubjectManifest {
        schema_version: 2,
        id: SubjectId::new("candidate").unwrap_or_else(|error| panic!("subject id: {error}")),
        display_name: "candidate".to_owned(),
        artifacts: versions
            .iter()
            .map(|version| SubjectArtifact {
                name: "kafkars".to_owned(),
                version: (*version).to_owned(),
                sha256: "a".repeat(64),
            })
            .collect(),
        command: "adapter".to_owned(),
        args: Vec::new(),
        environment: BTreeMap::new(),
        pass_environment: Vec::new(),
        working_directory: Some(".".to_owned()),
    }
}

fn descriptor(version: &str) -> AdapterDescriptor {
    AdapterDescriptor {
        id: AdapterId::new("kafkars-rust").unwrap_or_else(|error| panic!("adapter id: {error}")),
        implementation: "packaged kafkars Rust client".to_owned(),
        version: version.to_owned(),
        protocol_version: testlab_schema::PROTOCOL_VERSION,
        capabilities: BTreeSet::new(),
    }
}
