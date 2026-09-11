//! Consumer-group batch deletion protocol tests pin every caller-ordered identity.

use testlab_schema::{
    AdapterCommand, AdapterEvent, AdminConsumerGroupDeletionOutcome, AdminConsumerGroupsDeletion,
    ClientId, DeleteConsumerGroupsAction, OperationId, ScenarioAction,
};

use crate::runner_protocol::{EventDisposition, ExpectedEvent};

#[test]
fn translation_preserves_the_exact_caller_order() {
    let action = ScenarioAction::DeleteConsumerGroups(action());
    let Some((AdapterCommand::DeleteConsumerGroups(command), expected)) =
        crate::session_command_admin_group_batch::translate(&action)
    else {
        panic!("consumer-group batch deletion translation");
    };
    assert_eq!(command.client_id, client());
    assert_eq!(command.operation_id, operation());
    assert_eq!(command.group_ids, group_ids());
    assert_eq!(command.timeout_ms, 1_000);
    assert!(matches!(
        expected,
        ExpectedEvent::ConsumerGroupsDeleted { .. }
    ));
}

#[test]
fn completion_requires_every_group_identity_in_caller_order() {
    let expected = ExpectedEvent::ConsumerGroupsDeleted {
        operation_id: operation(),
        group_ids: group_ids(),
    };
    let mut event = AdapterEvent::ConsumerGroupsDeleted(completion());
    assert_eq!(
        expected
            .classify(&event)
            .unwrap_or_else(|error| panic!("classify consumer-group deletion: {error}")),
        EventDisposition::Complete
    );
    let AdapterEvent::ConsumerGroupsDeleted(actual) = &mut event else {
        panic!("consumer-group batch deletion event");
    };
    actual.outcomes.swap(0, 1);
    let error = expected
        .classify(&event)
        .expect_err("reordered groups must not complete");
    assert_eq!(error.harness_error().code, "event_identity_mismatch");
}

fn action() -> DeleteConsumerGroupsAction {
    DeleteConsumerGroupsAction {
        client_id: client(),
        operation_id: operation(),
        group_ids: group_ids(),
        timeout_ms: 1_000,
    }
}

fn completion() -> AdminConsumerGroupsDeletion {
    AdminConsumerGroupsDeletion {
        operation_id: operation(),
        outcomes: group_ids()
            .into_iter()
            .map(|group_id| AdminConsumerGroupDeletionOutcome {
                group_id,
                error_code: None,
            })
            .collect(),
    }
}

fn group_ids() -> Vec<String> {
    vec!["consumer-group-z".to_owned(), "consumer-group-a".to_owned()]
}

fn client() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client: {error}"))
}

fn operation() -> OperationId {
    OperationId::new("delete-consumer-groups").unwrap_or_else(|error| panic!("operation: {error}"))
}
