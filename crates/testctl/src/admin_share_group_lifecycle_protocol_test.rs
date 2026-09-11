//! Share-group lifecycle protocol tests pin caller order across command and completion.

use testlab_schema::{
    AdapterCommand, AdapterEvent, AdminShareGroupDeletionOutcome, AdminShareGroupsDeletion,
    ClientId, DeleteShareGroupsAction, OperationId, ScenarioAction,
};

use crate::runner_protocol::{EventDisposition, ExpectedEvent};

#[test]
fn translation_preserves_the_exact_caller_order() {
    let action = ScenarioAction::DeleteShareGroups(action());
    let Some((AdapterCommand::DeleteShareGroups(command), expected)) =
        crate::session_command_admin_share_group::translate(&action)
    else {
        panic!("Share-group deletion translation");
    };
    assert_eq!(command.client_id, client());
    assert_eq!(command.operation_id, operation());
    assert_eq!(command.group_ids, group_ids());
    assert_eq!(command.timeout_ms, 1_000);
    assert!(matches!(expected, ExpectedEvent::ShareGroupsDeleted { .. }));
}

#[test]
fn completion_requires_every_group_identity_in_caller_order() {
    let expected = ExpectedEvent::ShareGroupsDeleted {
        operation_id: operation(),
        group_ids: group_ids(),
    };
    let mut event = AdapterEvent::ShareGroupsDeleted(completion());
    assert_eq!(
        expected
            .classify(&event)
            .unwrap_or_else(|error| panic!("classify Share-group deletion: {error}")),
        EventDisposition::Complete
    );
    let AdapterEvent::ShareGroupsDeleted(actual) = &mut event else {
        panic!("Share-group deletion event");
    };
    actual.outcomes.swap(0, 1);
    let error = expected
        .classify(&event)
        .expect_err("reordered groups must not complete");
    assert_eq!(error.harness_error().code, "event_identity_mismatch");
}

fn action() -> DeleteShareGroupsAction {
    DeleteShareGroupsAction {
        client_id: client(),
        operation_id: operation(),
        group_ids: group_ids(),
        timeout_ms: 1_000,
    }
}

fn completion() -> AdminShareGroupsDeletion {
    AdminShareGroupsDeletion {
        operation_id: operation(),
        outcomes: group_ids()
            .into_iter()
            .map(|group_id| AdminShareGroupDeletionOutcome {
                group_id,
                error_code: None,
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
    OperationId::new("delete-share-groups").unwrap_or_else(|error| panic!("operation: {error}"))
}
