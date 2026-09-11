//! ACL verifier tests separate ordered public results from independent broker state.

use std::collections::BTreeSet;

use testlab_schema::{
    AclPermission, AdapterCommand, AdapterEvent, AdminAclCreationOutcome, AdminAclDeleteMatch,
    AdminAclDeletionOutcome, AdminAclsCreation, AdminAclsDeletion, AdminAclsDescription,
    BrokerAclOperation, BrokerAclResource, BrokerAclState, BrokerStateObservation, Capability,
    ClientId, CreateAclsAction, DeleteAclsAction, DescribeAclsAction, HistoryEntry, HistoryPayload,
    LiteralAclBinding, OperationId, SCENARIO_SCHEMA_VERSION, Scenario, ScenarioAction, ScenarioId,
};

use crate::admin::verify_admin;
use crate::index::HistoryIndex;
use crate::verify_fixture::{command, event, step};

#[test]
fn exact_public_acl_lifecycle_passes_with_immediate_independent_state() {
    assert!(violations(&history()).is_empty());
}

#[test]
fn reordered_creation_outcomes_fail_creation_contract() {
    let mut entries = history();
    let HistoryPayload::AdapterEvent { event } = &mut entries[1].payload else {
        panic!("creation event history kind");
    };
    let AdapterEvent::AclsCreated(created) = &mut event.event else {
        panic!("creation event kind");
    };
    created.outcomes.swap(0, 1);

    assert_contract(&violations(&entries), "ADMIN-030");
}

#[test]
fn wrong_description_and_post_delete_presence_fail_their_contracts() {
    let mut entries = history();
    let HistoryPayload::AdapterEvent { event } = &mut entries[5].payload else {
        panic!("description event history kind");
    };
    let AdapterEvent::AclsDescribed(described) = &mut event.event else {
        panic!("description event kind");
    };
    described.bindings.clear();
    let HistoryPayload::BrokerStateObservation { observation } = &mut entries[10].payload else {
        panic!("delete observation history kind");
    };
    let BrokerStateObservation::Acl(observed) = observation else {
        panic!("delete observation kind");
    };
    observed.present = true;

    let violations = violations(&entries);
    assert_contract(&violations, "ADMIN-031");
    assert_contract(&violations, "ADMIN-032");
}

fn history() -> Vec<HistoryEntry> {
    let values = bindings();
    vec![
        command(1, AdapterCommand::CreateAcls(create())),
        event(
            2,
            AdapterEvent::AclsCreated(AdminAclsCreation {
                operation_id: operation("acl-create"),
                outcomes: values
                    .iter()
                    .cloned()
                    .map(|binding| AdminAclCreationOutcome {
                        binding,
                        error_code: None,
                    })
                    .collect(),
            }),
        ),
        state(3, 0, "acl-create", values[0].clone(), true),
        state(4, 1, "acl-create", values[1].clone(), true),
        command(5, AdapterCommand::DescribeAcls(describe())),
        event(
            6,
            AdapterEvent::AclsDescribed(AdminAclsDescription {
                operation_id: operation("acl-describe"),
                bindings: vec![values[0].clone()],
            }),
        ),
        state(7, 2, "acl-describe", values[0].clone(), true),
        command(8, AdapterCommand::DeleteAcls(delete())),
        event(
            9,
            AdapterEvent::AclsDeleted(AdminAclsDeletion {
                operation_id: operation("acl-delete"),
                outcomes: values
                    .iter()
                    .cloned()
                    .map(|binding| AdminAclDeletionOutcome {
                        filter: binding.clone(),
                        error_code: None,
                        matches: vec![AdminAclDeleteMatch {
                            binding,
                            error_code: None,
                        }],
                    })
                    .collect(),
            }),
        ),
        state(10, 3, "acl-delete", values[0].clone(), false),
        state(11, 4, "acl-delete", values[1].clone(), false),
    ]
}

fn state(
    sequence: u64,
    observation: u64,
    operation_id: &str,
    binding: LiteralAclBinding,
    present: bool,
) -> HistoryEntry {
    HistoryEntry {
        sequence,
        observed_unix_ms: sequence,
        payload: HistoryPayload::BrokerStateObservation {
            observation: BrokerStateObservation::Acl(BrokerAclState {
                observation,
                operation_id: operation(operation_id),
                binding,
                present,
            }),
        },
    }
}

fn scenario() -> Scenario {
    Scenario {
        schema_version: SCENARIO_SCHEMA_VERSION,
        id: ScenarioId::new("kafka.admin-acl-lifecycle")
            .unwrap_or_else(|error| panic!("scenario: {error}")),
        title: "ACL lifecycle".to_owned(),
        description: "public ACL administration with independent broker state".to_owned(),
        timeout_ms: 10_000,
        requires: BTreeSet::from([Capability::Admin, Capability::Lifecycle]),
        steps: vec![
            step("create-acls", ScenarioAction::CreateAcls(create())),
            step("describe-acl", ScenarioAction::DescribeAcls(describe())),
            step("delete-acls", ScenarioAction::DeleteAcls(delete())),
        ],
        assertions: Vec::new(),
    }
}

fn create() -> CreateAclsAction {
    CreateAclsAction {
        client_id: client(),
        operation_id: operation("acl-create"),
        bindings: bindings(),
        timeout_ms: 1_000,
    }
}

fn describe() -> DescribeAclsAction {
    DescribeAclsAction {
        client_id: client(),
        operation_id: operation("acl-describe"),
        binding: bindings()[0].clone(),
        timeout_ms: 1_000,
    }
}

fn delete() -> DeleteAclsAction {
    DeleteAclsAction {
        client_id: client(),
        operation_id: operation("acl-delete"),
        bindings: bindings(),
        timeout_ms: 1_000,
    }
}

fn bindings() -> Vec<LiteralAclBinding> {
    vec![
        binding(BrokerAclResource::Topic {
            name: "orders".to_owned(),
        }),
        binding(BrokerAclResource::Group {
            name: "readers".to_owned(),
        }),
    ]
}

fn binding(resource: BrokerAclResource) -> LiteralAclBinding {
    LiteralAclBinding {
        resource,
        principal: "User:reader".to_owned(),
        operation: BrokerAclOperation::Read,
        permission: AclPermission::Allow,
    }
}

fn violations(entries: &[HistoryEntry]) -> Vec<testlab_schema::Violation> {
    let index = HistoryIndex::build(entries);
    let mut violations = Vec::new();
    verify_admin(&scenario(), &index, &[], &mut violations);
    violations
}

fn assert_contract(violations: &[testlab_schema::Violation], contract: &str) {
    assert!(
        violations
            .iter()
            .any(|violation| violation.contract_id.as_str() == contract),
        "{violations:?}"
    );
}

fn client() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client: {error}"))
}

fn operation(value: &str) -> OperationId {
    OperationId::new(value).unwrap_or_else(|error| panic!("operation: {error}"))
}
