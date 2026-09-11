//! ACL observer tests pin exact wire correlation and fail-closed CLI normalization.

use testlab_schema::{
    AclPermission, AdapterCommand, BrokerAclOperation, BrokerAclResource, BrokerStateObservation,
    ClientId, CreateAclsAction, LiteralAclBinding, OperationId, ScenarioAction,
};

use crate::observer_admin_target::{AclsTarget, AdminTarget};

#[test]
fn action_and_command_map_to_one_caller_ordered_acl_target() {
    let action = action();
    let command = command();
    let target = AdminTarget::from_exact(&action, &command)
        .unwrap_or_else(|error| panic!("ACL target: {error}"))
        .unwrap_or_else(|| panic!("missing ACL target"));

    let AdminTarget::Acls(target) = target else {
        panic!("ACL target kind");
    };
    assert_eq!(target.bindings, bindings());

    let AdapterCommand::CreateAcls(mut reordered) = command else {
        panic!("ACL command kind");
    };
    reordered.bindings.swap(0, 1);
    assert!(AdminTarget::from_exact(&action, &AdapterCommand::CreateAcls(reordered)).is_err());
}

#[test]
fn cli_normalization_distinguishes_exact_presence_from_absence() {
    let binding = bindings()[0].clone();
    let header = header(&binding);
    let present = format!(
        "{header}\n\t(principal=User:reader, host=*, operation=READ, permissionType=ALLOW)\n"
    );
    let present =
        crate::acl_cli_observation::normalize(4, &operation(), &binding, present.as_bytes())
            .unwrap_or_else(|error| panic!("present ACL: {error}"));
    let absent = crate::acl_cli_observation::normalize(
        5,
        &operation(),
        &binding,
        format!("{header}\n").as_bytes(),
    )
    .unwrap_or_else(|error| panic!("absent ACL: {error}"));

    assert_acl(present, 4, true, &binding);
    assert_acl(absent, 5, false, &binding);
}

#[test]
fn cli_normalization_rejects_unknown_partial_and_unsupported_output() {
    let binding = bindings()[0].clone();
    let header = header(&binding);
    for output in [
        String::new(),
        "Current ACLs for resource wrong".to_owned(),
        format!("{header}\n(principal=User:reader, operation=READ, permissionType=ALLOW)\n"),
        format!(
            "{header}\n(principal=User:reader, host=127.0.0.1, operation=READ, permissionType=ALLOW)\n"
        ),
        format!(
            "{header}\n(principal=User:reader, host=*, operation=ALTER, permissionType=ALLOW)\n"
        ),
        format!("{header}\nunexpected\n"),
    ] {
        assert!(
            crate::acl_cli_observation::normalize(0, &operation(), &binding, output.as_bytes(),)
                .is_err(),
            "{output:?}"
        );
    }
    assert!(crate::acl_cli_observation::normalize(0, &operation(), &binding, &[255]).is_err());
}

#[test]
fn cli_selection_uses_the_exact_resource_flag() {
    let values = bindings();
    assert_eq!(
        crate::acl_cli_observation::selection(&values[0]),
        ["--topic", "orders"]
    );
    assert_eq!(
        crate::acl_cli_observation::selection(&values[1]),
        ["--group", "readers"]
    );
}

#[test]
fn cli_query_retains_raw_terminal_and_rejects_duplicate_correlation() {
    let binding = bindings()[0].clone();
    let output = format!(
        "{}\\n\\t(principal=User:reader, host=*, operation=READ, permissionType=ALLOW)",
        header(&binding)
    );
    let target = AdminTarget::Acls(AclsTarget {
        operation_id: operation(),
        bindings: vec![binding],
    });
    let fixture = crate::compose_test_fixture::Fixture::new(false);
    let mut environment = fixture.environment();
    environment.program = "/bin/sh".into();
    environment.prefix = vec!["-c".to_owned(), format!("printf '%b\\n' '{output}'")];

    let observed = environment.observe_acls_with_cli(&target, std::time::Duration::from_secs(2));
    assert!(observed.phase.succeeded(), "{:?}", observed.phase.failure);
    assert_eq!(observed.phase.operations.len(), 1);
    assert_eq!(observed.phase.artifacts.len(), 2);
    assert_eq!(observed.state_observations.len(), 1);

    let duplicate = environment.observe_acls_with_cli(&target, std::time::Duration::from_secs(2));
    assert!(!duplicate.phase.succeeded());
    assert!(duplicate.phase.operations.is_empty());
}

fn assert_acl(
    observation: BrokerStateObservation,
    ordinal: u64,
    present: bool,
    binding: &LiteralAclBinding,
) {
    let BrokerStateObservation::Acl(value) = observation else {
        panic!("ACL observation kind");
    };
    assert_eq!(value.observation, ordinal);
    assert_eq!(value.operation_id, operation());
    assert_eq!(&value.binding, binding);
    assert_eq!(value.present, present);
}

fn header(binding: &LiteralAclBinding) -> String {
    let resource_type = match &binding.resource {
        BrokerAclResource::Topic { .. } => "TOPIC",
        BrokerAclResource::Group { .. } => "GROUP",
        BrokerAclResource::TransactionalId { .. } => "TRANSACTIONAL_ID",
    };
    format!(
        "Current ACLs for resource `ResourcePattern(resourceType={resource_type}, name={}, patternType=LITERAL)`:",
        binding.resource.name()
    )
}

fn action() -> ScenarioAction {
    ScenarioAction::CreateAcls(CreateAclsAction {
        client_id: client(),
        operation_id: operation(),
        bindings: bindings(),
        timeout_ms: 1_000,
    })
}

fn command() -> AdapterCommand {
    AdapterCommand::CreateAcls(CreateAclsAction {
        client_id: client(),
        operation_id: operation(),
        bindings: bindings(),
        timeout_ms: 1_000,
    })
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

fn client() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client: {error}"))
}

fn operation() -> OperationId {
    OperationId::new("acl-create").unwrap_or_else(|error| panic!("operation: {error}"))
}
