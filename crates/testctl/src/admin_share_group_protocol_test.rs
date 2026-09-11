//! Share-group protocol tests pin expectation-free commands and exact completion identity.

use testlab_schema::{
    AdapterCommand, AdapterEvent, AdminShareGroupDescription, ClientId, DescribeShareGroupAction,
    OperationId, ScenarioAction,
};

use crate::runner_protocol::{EventDisposition, ExpectedEvent};

#[test]
fn translation_keeps_scenario_expectations_off_the_wire() {
    let action = ScenarioAction::DescribeShareGroup(action());
    let Some((AdapterCommand::DescribeShareGroup(command), expected)) =
        crate::session_command_admin_share_group::translate(&action)
    else {
        panic!("Share-group description translation");
    };
    assert_eq!(command.client_id, client());
    assert_eq!(command.operation_id, operation());
    assert_eq!(command.group_id, "share-group-1");
    assert_eq!(command.timeout_ms, 1_000);
    assert!(matches!(
        expected,
        ExpectedEvent::ShareGroupDescribed { .. }
    ));
}

#[test]
fn completion_requires_exact_operation_and_group_identities() {
    let expected = ExpectedEvent::ShareGroupDescribed {
        operation_id: operation(),
        group_id: "share-group-1".to_owned(),
    };
    let mut event = AdapterEvent::ShareGroupDescribed(description());
    assert_eq!(
        expected
            .classify(&event)
            .unwrap_or_else(|error| panic!("classify Share-group event: {error}")),
        EventDisposition::Complete
    );
    let AdapterEvent::ShareGroupDescribed(value) = &mut event else {
        panic!("Share-group event kind");
    };
    value.group_id = "other-group".to_owned();
    let error = expected
        .classify(&event)
        .expect_err("foreign group must not complete");
    assert_eq!(error.harness_error().code, "event_identity_mismatch");
    value.group_id = "share-group-1".to_owned();
    value.operation_id = OperationId::new("other-operation")
        .unwrap_or_else(|error| panic!("other operation: {error}"));
    let error = expected
        .classify(&event)
        .expect_err("foreign operation must not complete");
    assert_eq!(error.harness_error().code, "event_identity_mismatch");
}

fn action() -> DescribeShareGroupAction {
    DescribeShareGroupAction {
        client_id: client(),
        operation_id: operation(),
        group_id: "share-group-1".to_owned(),
        expected_state: "Stable".to_owned(),
        expected_member_count: 1,
        expected_topic: "share-topic".to_owned(),
        expected_partition: 0,
        timeout_ms: 1_000,
    }
}

fn description() -> AdminShareGroupDescription {
    AdminShareGroupDescription {
        operation_id: operation(),
        group_id: "share-group-1".to_owned(),
        state: "Stable".to_owned(),
        group_epoch: 1,
        assignment_epoch: 1,
        assignor_name: "simple".to_owned(),
        members: Vec::new(),
    }
}

fn client() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client: {error}"))
}

fn operation() -> OperationId {
    OperationId::new("describe-share-group").unwrap_or_else(|error| panic!("operation: {error}"))
}
