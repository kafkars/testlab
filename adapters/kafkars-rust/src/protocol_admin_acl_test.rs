//! ACL mapping tests pin Kafkars' public model to Testlab's supported subset.

use testlab_schema::{
    AclPermission, BrokerAclOperation, BrokerAclResource, LiteralAclBinding, OperationId,
};

use crate::kafkars_api::{
    AccessControlEntry, AclBinding, AclBindingFilter, AclOperation, AclPatternType,
    AclPermissionType, AclResourceType, ResourcePattern,
};
use crate::protocol_admin_acl_mapping::{public_binding, schema_binding, schema_filter};

#[test]
fn supported_bindings_and_exact_filters_round_trip() {
    for binding in bindings() {
        let public = public_binding(&binding);
        let filter = AclBindingFilter::from_binding(&public);

        assert_eq!(
            schema_binding(&public, &operation())
                .unwrap_or_else(|error| panic!("binding mapping: {error}")),
            binding
        );
        assert_eq!(
            schema_filter(&filter, &operation())
                .unwrap_or_else(|error| panic!("filter mapping: {error}")),
            binding
        );
    }
}

#[test]
fn nonliteral_non_wildcard_and_nonexact_public_values_are_rejected() {
    let prefixed = AclBinding::new(
        ResourcePattern::new(AclResourceType::TOPIC, "orders", AclPatternType::PREFIXED),
        AccessControlEntry::new(
            "User:reader",
            "*",
            AclOperation::READ,
            AclPermissionType::ALLOW,
        ),
    );
    let concrete_host = AclBinding::new(
        ResourcePattern::new(AclResourceType::TOPIC, "orders", AclPatternType::LITERAL),
        AccessControlEntry::new(
            "User:reader",
            "127.0.0.1",
            AclOperation::READ,
            AclPermissionType::ALLOW,
        ),
    );
    let nonexact = AclBindingFilter::new(
        AclResourceType::TOPIC,
        AclPatternType::LITERAL,
        AclOperation::READ,
        AclPermissionType::ALLOW,
    )
    .with_resource_name("orders")
    .with_principal("User:reader");

    assert!(schema_binding(&prefixed, &operation()).is_err());
    assert!(schema_binding(&concrete_host, &operation()).is_err());
    assert!(schema_filter(&nonexact, &operation()).is_err());
}

fn bindings() -> Vec<LiteralAclBinding> {
    vec![
        binding(
            BrokerAclResource::Topic {
                name: "orders".to_owned(),
            },
            "User:reader",
            BrokerAclOperation::Read,
            AclPermission::Allow,
        ),
        binding(
            BrokerAclResource::Group {
                name: "readers".to_owned(),
            },
            "User:reader",
            BrokerAclOperation::Create,
            AclPermission::Deny,
        ),
        binding(
            BrokerAclResource::TransactionalId {
                name: "writer-txn".to_owned(),
            },
            "User:writer",
            BrokerAclOperation::Write,
            AclPermission::Allow,
        ),
    ]
}

fn binding(
    resource: BrokerAclResource,
    principal: &str,
    operation: BrokerAclOperation,
    permission: AclPermission,
) -> LiteralAclBinding {
    LiteralAclBinding {
        resource,
        principal: principal.to_owned(),
        operation,
        permission,
    }
}

fn operation() -> OperationId {
    OperationId::new("acl-map").unwrap_or_else(|error| panic!("operation: {error}"))
}
