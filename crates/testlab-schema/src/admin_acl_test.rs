//! ACL schema tests pin bounded input and caller-ordered public evidence.

use std::collections::{BTreeMap, BTreeSet};

use super::{
    AclPermission, AdapterCommand, AdapterEvent, AdminAclCreationOutcome, AdminAclDeleteMatch,
    AdminAclDeletionOutcome, AdminAclsCreation, AdminAclsDeletion, AdminAclsDescription,
    BrokerAclOperation, BrokerAclResource, BrokerAclState, BrokerStateObservation, ClientId,
    CreateAclsAction, DeleteAclsAction, DescribeAclsAction, EVIDENCE_SCHEMA_VERSION,
    LiteralAclBinding, OperationId, PROTOCOL_VERSION, SCENARIO_SCHEMA_VERSION, ScenarioAction,
};

#[test]
fn acl_cut_advances_all_three_versioned_boundaries() {
    assert_eq!(PROTOCOL_VERSION, 113);
    assert_eq!(SCENARIO_SCHEMA_VERSION, 116);
    assert_eq!(EVIDENCE_SCHEMA_VERSION, 102);
}

#[test]
fn actions_commands_public_results_and_broker_facts_round_trip() {
    let values = bindings();
    let actions = [
        ScenarioAction::CreateAcls(CreateAclsAction {
            client_id: client(),
            operation_id: operation("acl-create"),
            bindings: values.clone(),
            timeout_ms: 1_000,
        }),
        ScenarioAction::DescribeAcls(DescribeAclsAction {
            client_id: client(),
            operation_id: operation("acl-describe"),
            binding: values[0].clone(),
            timeout_ms: 1_000,
        }),
        ScenarioAction::DeleteAcls(DeleteAclsAction {
            client_id: client(),
            operation_id: operation("acl-delete"),
            bindings: values.clone(),
            timeout_ms: 1_000,
        }),
    ];
    for action in actions {
        let encoded =
            toml::to_string(&action).unwrap_or_else(|error| panic!("encode ACL action: {error}"));
        let decoded = toml::from_str::<ScenarioAction>(&encoded)
            .unwrap_or_else(|error| panic!("decode ACL action: {error}"));
        assert_eq!(decoded, action);
    }

    let commands = [
        AdapterCommand::CreateAcls(create()),
        AdapterCommand::DescribeAcls(describe()),
        AdapterCommand::DeleteAcls(delete()),
    ];
    for command in commands {
        round_trip_json(&command);
    }
    for event in events() {
        round_trip_json(&event);
    }
    round_trip_json(&BrokerStateObservation::Acl(BrokerAclState {
        observation: 7,
        operation_id: operation("acl-create"),
        binding: values[0].clone(),
        present: true,
    }));
}

#[test]
fn validation_rejects_empty_duplicate_and_unsafe_bindings() {
    let clients = BTreeMap::from([(client(), false)]);
    let mut operation_ids = BTreeSet::new();
    let mut problems = Vec::new();
    crate::admin_action_validation::validate(
        &ScenarioAction::CreateAcls(create()),
        &clients,
        &mut operation_ids,
        &mut problems,
    );
    assert!(problems.is_empty(), "{problems:?}");

    let mut unsafe_binding = bindings()[0].clone();
    unsafe_binding.principal = "User:bad principal".to_owned();
    let excessive = (0..33)
        .map(|index| LiteralAclBinding {
            resource: BrokerAclResource::Topic {
                name: if index == 0 {
                    "bad topic".to_owned()
                } else {
                    format!("topic-{index}")
                },
            },
            principal: "User:reader".to_owned(),
            operation: BrokerAclOperation::Read,
            permission: AclPermission::Allow,
        })
        .collect();
    let invalid = [
        ScenarioAction::DeleteAcls(DeleteAclsAction {
            client_id: client(),
            operation_id: operation("acl-empty"),
            bindings: Vec::new(),
            timeout_ms: 99,
        }),
        ScenarioAction::CreateAcls(CreateAclsAction {
            client_id: client(),
            operation_id: operation("acl-duplicate"),
            bindings: vec![unsafe_binding.clone(), unsafe_binding],
            timeout_ms: 1_000,
        }),
        ScenarioAction::CreateAcls(CreateAclsAction {
            client_id: client(),
            operation_id: operation("acl-too-many"),
            bindings: excessive,
            timeout_ms: 1_000,
        }),
    ];
    for action in invalid {
        crate::admin_action_validation::validate(
            &action,
            &clients,
            &mut operation_ids,
            &mut problems,
        );
    }
    for expected in [
        "bindings must contain 1 to 32",
        "timeout_ms must be between 100 and 60000",
        "invalid ACL principal",
        "invalid ACL resource",
        "ACL bindings must be unique",
    ] {
        assert!(
            problems.iter().any(|problem| problem.contains(expected)),
            "missing {expected:?} in {problems:?}"
        );
    }
}

fn events() -> [AdapterEvent; 3] {
    let values = bindings();
    [
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
        AdapterEvent::AclsDescribed(AdminAclsDescription {
            operation_id: operation("acl-describe"),
            bindings: vec![values[0].clone()],
        }),
        AdapterEvent::AclsDeleted(AdminAclsDeletion {
            operation_id: operation("acl-delete"),
            outcomes: values
                .into_iter()
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
    ]
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
        LiteralAclBinding {
            resource: BrokerAclResource::Topic {
                name: "orders".to_owned(),
            },
            principal: "User:reader".to_owned(),
            operation: BrokerAclOperation::Read,
            permission: AclPermission::Allow,
        },
        LiteralAclBinding {
            resource: BrokerAclResource::TransactionalId {
                name: "writer-txn".to_owned(),
            },
            principal: "User:writer".to_owned(),
            operation: BrokerAclOperation::Write,
            permission: AclPermission::Deny,
        },
    ]
}

fn round_trip_json<T>(value: &T)
where
    T: serde::Serialize + serde::de::DeserializeOwned + std::fmt::Debug + PartialEq,
{
    let encoded =
        serde_json::to_string(value).unwrap_or_else(|error| panic!("encode ACL value: {error}"));
    let decoded = serde_json::from_str::<T>(&encoded)
        .unwrap_or_else(|error| panic!("decode ACL value: {error}"));
    assert_eq!(&decoded, value);
}

fn client() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client: {error}"))
}

fn operation(value: &str) -> OperationId {
    OperationId::new(value).unwrap_or_else(|error| panic!("operation: {error}"))
}
