//! Share-group protocol tests pin expectation-free commands and exact completion identity.

use testlab_schema::{
    AdapterCommand, AdapterEvent, AdminShareGroupDescription, AdminShareGroupOffsetListing,
    ClientId, DescribeShareGroupAction, ListShareGroupOffsetsAction, OperationId, ScenarioAction,
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
    assert!(command.include_authorized_operations);
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
    {
        let AdapterEvent::ShareGroupDescribed(value) = &mut event else {
            panic!("Share-group event kind");
        };
        value.group_id = "other-group".to_owned();
    }
    let error = expected
        .classify(&event)
        .err()
        .unwrap_or_else(|| panic!("foreign group must not complete"));
    assert_eq!(error.harness_error().code, "event_identity_mismatch");
    {
        let AdapterEvent::ShareGroupDescribed(value) = &mut event else {
            panic!("Share-group event kind");
        };
        value.group_id = "share-group-1".to_owned();
        value.operation_id = OperationId::new("other-operation")
            .unwrap_or_else(|error| panic!("other operation: {error}"));
    }
    let error = expected
        .classify(&event)
        .err()
        .unwrap_or_else(|| panic!("foreign operation must not complete"));
    assert_eq!(error.harness_error().code, "event_identity_mismatch");
}

#[test]
fn offset_translation_omits_expectations_and_requires_exact_partition_identity() {
    let action = ScenarioAction::ListShareGroupOffsets(offset_action());
    let Some((AdapterCommand::ListShareGroupOffsets(command), expected)) =
        crate::session_command_admin_share_group::translate(&action)
    else {
        panic!("Share-group offset translation");
    };
    assert_eq!(command.client_id, client());
    assert_eq!(command.operation_id, offset_operation());
    assert_eq!(command.group_id, "share-group-1");
    assert_eq!(command.topic, "share-topic");
    assert_eq!(command.partition, 0);
    assert_eq!(command.timeout_ms, 1_000);

    let mut event = AdapterEvent::ShareGroupOffsetsListed(offset_listing());
    assert_eq!(
        expected
            .classify(&event)
            .unwrap_or_else(|error| panic!("classify Share-group offset event: {error}")),
        EventDisposition::Complete
    );
    let AdapterEvent::ShareGroupOffsetsListed(value) = &mut event else {
        panic!("Share-group offset event kind");
    };
    value.partition = 1;
    let error = expected
        .classify(&event)
        .err()
        .unwrap_or_else(|| panic!("foreign partition must not complete"));
    assert_eq!(error.harness_error().code, "event_identity_mismatch");
}

fn action() -> DescribeShareGroupAction {
    DescribeShareGroupAction {
        client_id: client(),
        operation_id: operation(),
        group_id: "share-group-1".to_owned(),
        expected_state: "Stable".to_owned(),
        expected_member_count: 1,
        expected_rack_id: Some("rack-a".to_owned()),
        expected_topic: "share-topic".to_owned(),
        expected_partition: 0,
        include_authorized_operations: true,
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
        authorized_operations: Some(1),
        members: Vec::new(),
    }
}

fn offset_action() -> ListShareGroupOffsetsAction {
    ListShareGroupOffsetsAction {
        client_id: client(),
        operation_id: offset_operation(),
        group_id: "share-group-1".to_owned(),
        topic: "share-topic".to_owned(),
        partition: 0,
        expected_start_offset: 1,
        expected_lag: 1,
        timeout_ms: 1_000,
    }
}

fn offset_listing() -> AdminShareGroupOffsetListing {
    AdminShareGroupOffsetListing {
        operation_id: offset_operation(),
        group_id: "share-group-1".to_owned(),
        topic: "share-topic".to_owned(),
        partition: 0,
        topic_id: [1; 16],
        start_offset: Some(1),
        leader_epoch: Some(0),
        lag: Some(1),
        error_code: None,
    }
}

fn client() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client: {error}"))
}

fn operation() -> OperationId {
    OperationId::new("describe-share-group").unwrap_or_else(|error| panic!("operation: {error}"))
}

fn offset_operation() -> OperationId {
    OperationId::new("list-share-group-offsets")
        .unwrap_or_else(|error| panic!("operation: {error}"))
}
