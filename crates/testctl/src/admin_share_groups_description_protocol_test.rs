//! Plural Share-group protocol tests pin expectation-free caller-ordered identities.

use testlab_schema::{
    AdapterCommand, AdapterEvent, AdminShareGroupDescriptionOutcome, AdminShareGroupsDescription,
    ClientId, DescribeShareGroupsAction, OperationId, ScenarioAction,
    ShareGroupDescriptionExpectation,
};

use crate::runner_protocol::{EventDisposition, ExpectedEvent};

#[test]
fn translation_preserves_caller_order_without_scenario_expectations() {
    let action = ScenarioAction::DescribeShareGroups(action());
    let Some((AdapterCommand::DescribeShareGroups(command), expected)) =
        crate::session_command_admin_share_group::translate(&action)
    else {
        panic!("plural Share-group description translation");
    };
    assert_eq!(command.client_id, client());
    assert_eq!(command.operation_id, operation());
    assert_eq!(command.group_ids, group_ids());
    assert_eq!(command.timeout_ms, 1_000);
    assert!(matches!(
        expected,
        ExpectedEvent::ShareGroupsDescribed { .. }
    ));
}

#[test]
fn completion_requires_every_group_identity_in_caller_order() {
    let expected = ExpectedEvent::ShareGroupsDescribed {
        operation_id: operation(),
        group_ids: group_ids(),
    };
    let mut event = AdapterEvent::ShareGroupsDescribed(completion());
    assert_eq!(
        expected
            .classify(&event)
            .unwrap_or_else(|error| panic!("classify plural Share description: {error}")),
        EventDisposition::Complete
    );
    let AdapterEvent::ShareGroupsDescribed(actual) = &mut event else {
        panic!("plural Share-group description event");
    };
    actual.outcomes.swap(0, 1);
    let error = expected
        .classify(&event)
        .expect_err("reordered groups must not complete");
    assert_eq!(error.harness_error().code, "event_identity_mismatch");
}

fn action() -> DescribeShareGroupsAction {
    DescribeShareGroupsAction {
        client_id: client(),
        operation_id: operation(),
        groups: [("share-group-z", "topic-z"), ("share-group-a", "topic-a")]
            .into_iter()
            .map(|(group_id, topic)| ShareGroupDescriptionExpectation {
                group_id: group_id.to_owned(),
                expected_state: "Stable".to_owned(),
                expected_member_count: 1,
                expected_topic: topic.to_owned(),
                expected_partition: 0,
            })
            .collect(),
        timeout_ms: 1_000,
    }
}

fn completion() -> AdminShareGroupsDescription {
    AdminShareGroupsDescription {
        operation_id: operation(),
        outcomes: group_ids()
            .into_iter()
            .map(|group_id| AdminShareGroupDescriptionOutcome {
                group_id,
                description: None,
                error_code: Some("test-error".to_owned()),
            })
            .collect(),
    }
}

fn group_ids() -> Vec<String> {
    vec!["share-group-z".to_owned(), "share-group-a".to_owned()]
}

fn client() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client: {error}"))
}

fn operation() -> OperationId {
    OperationId::new("describe-share-groups").unwrap_or_else(|error| panic!("operation: {error}"))
}
