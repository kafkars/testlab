//! Static-member removal protocol tests pin every caller-ordered instance identity.

use testlab_schema::{
    AdapterCommand, AdapterEvent, AdminConsumerGroupMemberRemovalOutcome,
    AdminConsumerGroupMembersRemoval, ClientId, OperationId, RemoveConsumerGroupMembersAction,
    ScenarioAction,
};

use crate::runner_protocol::{EventDisposition, ExpectedEvent};

#[test]
fn translation_preserves_reason_and_exact_caller_order_without_the_baseline() {
    let action = ScenarioAction::RemoveConsumerGroupMembers(action());
    let Some((AdapterCommand::RemoveConsumerGroupMembers(command), expected)) =
        crate::session_command_admin_group_batch::translate(&action)
    else {
        panic!("static-member removal translation");
    };
    assert_eq!(command.client_id, client());
    assert_eq!(command.operation_id, operation());
    assert_eq!(command.group_id, "workers");
    assert_eq!(command.group_instance_ids, identities());
    assert_eq!(command.reason, "testlab static cleanup");
    assert_eq!(command.timeout_ms, 1_000);
    assert!(matches!(
        expected,
        ExpectedEvent::ConsumerGroupMembersRemoved(..)
    ));
}

#[test]
fn completion_requires_every_instance_identity_in_caller_order() {
    let expected = ExpectedEvent::ConsumerGroupMembersRemoved(operation(), identities());
    let mut event = AdapterEvent::ConsumerGroupMembersRemoved(completion());
    assert_eq!(
        expected
            .classify(&event)
            .unwrap_or_else(|error| panic!("classify static-member removal: {error}")),
        EventDisposition::Complete
    );
    let AdapterEvent::ConsumerGroupMembersRemoved(actual) = &mut event else {
        panic!("static-member removal event");
    };
    actual.outcomes.swap(0, 1);
    let error = expected
        .classify(&event)
        .err()
        .unwrap_or_else(|| panic!("reordered static members must not complete"));
    assert_eq!(error.harness_error().code, "event_identity_mismatch");
}

fn action() -> RemoveConsumerGroupMembersAction {
    RemoveConsumerGroupMembersAction {
        client_id: client(),
        operation_id: operation(),
        baseline_operation_id: OperationId::new("describe-static-members")
            .unwrap_or_else(|error| panic!("baseline operation: {error}")),
        group_id: "workers".to_owned(),
        group_instance_ids: identities(),
        reason: "testlab static cleanup".to_owned(),
        timeout_ms: 1_000,
    }
}

fn completion() -> AdminConsumerGroupMembersRemoval {
    AdminConsumerGroupMembersRemoval {
        operation_id: operation(),
        group_id: "workers".to_owned(),
        throttle_time_ms: 7,
        outcomes: identities()
            .into_iter()
            .map(|group_instance_id| AdminConsumerGroupMemberRemovalOutcome {
                group_instance_id,
                error_code: None,
            })
            .collect(),
    }
}

fn identities() -> Vec<String> {
    vec!["static-zulu".to_owned(), "static-alpha".to_owned()]
}

fn client() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client: {error}"))
}

fn operation() -> OperationId {
    OperationId::new("remove-static-members").unwrap_or_else(|error| panic!("operation: {error}"))
}
