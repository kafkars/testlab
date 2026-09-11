//! Plural Share offset protocol tests pin expectation-free caller-ordered identities.

use testlab_schema::{
    AdapterCommand, AdapterEvent, AdminShareGroupOffsetsOutcome, AdminShareGroupsOffsetsListing,
    ClientId, ListShareGroupsOffsetsAction, OperationId, ScenarioAction,
    ShareGroupOffsetExpectation, ShareGroupOffsetsExpectation,
};

use crate::runner_protocol::{EventDisposition, ExpectedEvent};

#[test]
fn translation_preserves_nested_order_without_scenario_expectations() {
    let action = ScenarioAction::ListShareGroupsOffsets(action());
    let Some((AdapterCommand::ListShareGroupsOffsets(command), expected)) =
        crate::session_command_admin_share_group::translate(&action)
    else {
        panic!("plural Share offset translation");
    };
    assert_eq!(command.client_id, client());
    assert_eq!(command.operation_id, operation());
    assert_eq!(
        command
            .groups
            .iter()
            .map(|group| group.group_id.as_str())
            .collect::<Vec<_>>(),
        vec!["share-z", "share-a"]
    );
    assert_eq!(command.groups[0].partitions[0].topic, "topic-z");
    assert_eq!(command.groups[0].partitions[0].partition, 1);
    assert_eq!(command.timeout_ms, 1_000);
    assert!(matches!(
        expected,
        ExpectedEvent::ShareGroupsOffsetsListed { .. }
    ));
}

#[test]
fn completion_requires_every_group_identity_in_caller_order() {
    let expected = ExpectedEvent::ShareGroupsOffsetsListed {
        operation_id: operation(),
        group_ids: group_ids(),
    };
    let mut event = AdapterEvent::ShareGroupsOffsetsListed(completion());
    assert_eq!(
        expected
            .classify(&event)
            .unwrap_or_else(|error| panic!("classify plural Share offsets: {error}")),
        EventDisposition::Complete
    );
    let AdapterEvent::ShareGroupsOffsetsListed(actual) = &mut event else {
        panic!("plural Share offsets event");
    };
    actual.groups.swap(0, 1);
    let error = expected
        .classify(&event)
        .expect_err("reordered groups must not complete");
    assert_eq!(error.harness_error().code, "event_identity_mismatch");
}

fn action() -> ListShareGroupsOffsetsAction {
    ListShareGroupsOffsetsAction {
        client_id: client(),
        operation_id: operation(),
        groups: [
            ("share-z", "topic-z", 1, 3, 5),
            ("share-a", "topic-a", 0, 2, 7),
        ]
        .into_iter()
        .map(
            |(group_id, topic, partition, expected_start_offset, expected_lag)| {
                ShareGroupOffsetsExpectation {
                    group_id: group_id.to_owned(),
                    partitions: vec![ShareGroupOffsetExpectation {
                        topic: topic.to_owned(),
                        partition,
                        expected_start_offset,
                        expected_lag,
                    }],
                }
            },
        )
        .collect(),
        timeout_ms: 1_000,
    }
}

fn completion() -> AdminShareGroupsOffsetsListing {
    AdminShareGroupsOffsetsListing {
        operation_id: operation(),
        groups: group_ids()
            .into_iter()
            .map(|group_id| AdminShareGroupOffsetsOutcome {
                group_id,
                error_code: Some("test-error".to_owned()),
                offsets: Vec::new(),
            })
            .collect(),
    }
}

fn group_ids() -> Vec<String> {
    vec!["share-z".to_owned(), "share-a".to_owned()]
}

fn client() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client: {error}"))
}

fn operation() -> OperationId {
    OperationId::new("list-share-groups-offsets")
        .unwrap_or_else(|error| panic!("operation: {error}"))
}
