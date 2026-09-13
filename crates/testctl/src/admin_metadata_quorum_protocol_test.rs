//! Metadata-quorum protocol tests pin exact command and completion identities.

use testlab_schema::{
    AdapterCommand, AdapterEvent, AdminMetadataQuorumDescription, ClientId,
    DescribeMetadataQuorumAction, OperationId, ScenarioAction,
};

use crate::runner_protocol::{EventDisposition, ExpectedEvent};

#[test]
fn translation_and_completion_preserve_the_operation_identity() {
    let action = ScenarioAction::DescribeMetadataQuorum(DescribeMetadataQuorumAction {
        client_id: client(),
        operation_id: operation(),
        timeout_ms: 1_000,
    });
    let Some((AdapterCommand::DescribeMetadataQuorum(command), expected)) =
        crate::session_command_admin::translate(&action)
    else {
        panic!("metadata-quorum translation");
    };
    assert_eq!(command.client_id, client());
    assert_eq!(command.operation_id, operation());
    assert_eq!(command.timeout_ms, 1_000);
    let event = AdapterEvent::MetadataQuorumDescribed(description());
    assert_eq!(
        expected
            .classify(&event)
            .unwrap_or_else(|error| panic!("classify metadata-quorum event: {error}")),
        EventDisposition::Complete
    );
}

#[test]
fn completion_rejects_a_foreign_operation() {
    let expected = ExpectedEvent::MetadataQuorumDescribed(operation());
    let mut event = AdapterEvent::MetadataQuorumDescribed(description());
    let AdapterEvent::MetadataQuorumDescribed(actual) = &mut event else {
        panic!("metadata-quorum event");
    };
    actual.operation_id = OperationId::new("foreign").unwrap_or_else(|error| panic!("id: {error}"));
    let error = expected
        .classify(&event)
        .err()
        .unwrap_or_else(|| panic!("foreign operation must not complete"));
    assert_eq!(error.harness_error().code, "event_identity_mismatch");
}

fn description() -> AdminMetadataQuorumDescription {
    AdminMetadataQuorumDescription {
        operation_id: operation(),
        leader_id: Some(1),
        leader_epoch: 2,
        high_watermark: 3,
        voters: Vec::new(),
        observers: Vec::new(),
        nodes: None,
    }
}

fn client() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client: {error}"))
}

fn operation() -> OperationId {
    OperationId::new("describe-metadata-quorum")
        .unwrap_or_else(|error| panic!("operation: {error}"))
}
