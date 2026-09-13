//! ACL protocol tests pin exact translation, identity, and event-family correlation.

use testlab_schema::{
    AclPermission, AdapterCommand, AdapterEvent, AdminAclsCreation, AdminAclsDeletion,
    AdminAclsDescription, BrokerAclOperation, BrokerAclResource, ClientId, CreateAclsAction,
    DeleteAclsAction, DescribeAclsAction, LiteralAclBinding, OperationId, ScenarioAction,
};

use crate::runner_protocol::{EventDisposition, ExpectedEvent};

#[test]
fn acl_actions_translate_without_changing_public_intent() {
    for (action, command) in actions().into_iter().zip(commands()) {
        let Some((actual, _)) = crate::session_command_admin_acl::translate(&action) else {
            panic!("ACL action must translate");
        };
        assert_eq!(actual, command);
    }
}

#[test]
fn acl_completions_require_exact_operation_and_event_family() {
    let expected = expected_events("acl-operation");
    let actual_events = events("acl-operation");
    for (expected_index, expected) in expected.iter().enumerate() {
        for (event_index, event) in actual_events.iter().enumerate() {
            let result = expected.classify(event);
            if expected_index == event_index {
                assert_eq!(
                    result.unwrap_or_else(|error| panic!("ACL classification: {error}")),
                    EventDisposition::Complete
                );
            } else {
                let error = match result {
                    Ok(disposition) => {
                        panic!("cross-family ACL event completed as {disposition:?}")
                    }
                    Err(error) => error,
                };
                assert_eq!(error.harness_error().code, "event_identity_mismatch");
            }
        }
    }

    for (expected, event) in expected.iter().zip(events("other-operation")) {
        let error = match expected.classify(&event) {
            Ok(disposition) => panic!("foreign ACL identity completed as {disposition:?}"),
            Err(error) => error,
        };
        assert_eq!(error.harness_error().code, "event_identity_mismatch");
    }
}

fn actions() -> Vec<ScenarioAction> {
    vec![
        ScenarioAction::CreateAcls(create()),
        ScenarioAction::DescribeAcls(describe()),
        ScenarioAction::DeleteAcls(delete()),
    ]
}

fn commands() -> Vec<AdapterCommand> {
    vec![
        AdapterCommand::CreateAcls(create()),
        AdapterCommand::DescribeAcls(describe()),
        AdapterCommand::DeleteAcls(delete()),
    ]
}

fn expected_events(operation_id: &str) -> Vec<ExpectedEvent> {
    vec![
        ExpectedEvent::AclsCreated(operation(operation_id)),
        ExpectedEvent::AclsDescribed(operation(operation_id)),
        ExpectedEvent::AclsDeleted(operation(operation_id)),
    ]
}

fn events(operation_id: &str) -> Vec<AdapterEvent> {
    vec![
        AdapterEvent::AclsCreated(AdminAclsCreation {
            operation_id: operation(operation_id),
            outcomes: Vec::new(),
        }),
        AdapterEvent::AclsDescribed(AdminAclsDescription {
            operation_id: operation(operation_id),
            bindings: Vec::new(),
        }),
        AdapterEvent::AclsDeleted(AdminAclsDeletion {
            operation_id: operation(operation_id),
            outcomes: Vec::new(),
        }),
    ]
}

fn create() -> CreateAclsAction {
    CreateAclsAction {
        client_id: client(),
        operation_id: operation("acl-create"),
        bindings: vec![binding()],
        timeout_ms: 1_000,
    }
}

fn describe() -> DescribeAclsAction {
    DescribeAclsAction {
        client_id: client(),
        operation_id: operation("acl-describe"),
        binding: binding(),
        timeout_ms: 1_000,
    }
}

fn delete() -> DeleteAclsAction {
    DeleteAclsAction {
        client_id: client(),
        operation_id: operation("acl-delete"),
        bindings: vec![binding()],
        timeout_ms: 1_000,
    }
}

fn binding() -> LiteralAclBinding {
    LiteralAclBinding {
        resource: BrokerAclResource::Topic {
            name: "orders".to_owned(),
        },
        principal: "User:reader".to_owned(),
        operation: BrokerAclOperation::Read,
        permission: AclPermission::Allow,
    }
}

fn client() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client: {error}"))
}

fn operation(value: &str) -> OperationId {
    OperationId::new(value).unwrap_or_else(|error| panic!("operation: {error}"))
}
