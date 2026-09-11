//! ACL mapping rejects every public value outside Testlab's literal wildcard-host subset.

use testlab_schema::{AclPermission, LiteralAclBinding, OperationId};

use crate::AdapterError;
use crate::kafkars_api::{
    AccessControlEntry, AclBinding, AclBindingFilter, AclOperation, AclPatternType,
    AclPermissionType, AclResourceType, ResourcePattern,
};

pub(crate) fn public_binding(binding: &LiteralAclBinding) -> AclBinding {
    AclBinding::new(
        ResourcePattern::new(
            resource_type(&binding.resource),
            binding.resource.name(),
            AclPatternType::LITERAL,
        ),
        AccessControlEntry::new(
            binding.principal.clone(),
            "*",
            operation(binding.operation),
            permission(binding.permission),
        ),
    )
}

pub(crate) fn schema_binding(
    binding: &AclBinding,
    operation_id: &OperationId,
) -> Result<LiteralAclBinding, AdapterError> {
    let pattern = binding.pattern();
    let entry = binding.entry();
    if pattern.pattern_type() != AclPatternType::LITERAL || entry.host() != "*" {
        return Err(invalid_result(
            operation_id,
            "returned an unsupported ACL binding",
        ));
    }
    Ok(LiteralAclBinding {
        resource: schema_resource(pattern.resource_type(), pattern.name(), operation_id)?,
        principal: entry.principal().to_owned(),
        operation: schema_operation(entry.operation(), operation_id)?,
        permission: schema_permission(entry.permission_type(), operation_id)?,
    })
}

pub(crate) fn schema_filter(
    filter: &AclBindingFilter,
    operation_id: &OperationId,
) -> Result<LiteralAclBinding, AdapterError> {
    let (Some(name), Some(principal), Some("*")) =
        (filter.resource_name(), filter.principal(), filter.host())
    else {
        return Err(invalid_result(
            operation_id,
            "returned a non-exact ACL filter",
        ));
    };
    if filter.pattern_type() != AclPatternType::LITERAL {
        return Err(invalid_result(
            operation_id,
            "returned a non-literal ACL filter",
        ));
    }
    Ok(LiteralAclBinding {
        resource: schema_resource(filter.resource_type(), name, operation_id)?,
        principal: principal.to_owned(),
        operation: schema_operation(filter.operation(), operation_id)?,
        permission: schema_permission(filter.permission_type(), operation_id)?,
    })
}

pub(crate) fn invalid_result(operation_id: &OperationId, detail: &str) -> AdapterError {
    AdapterError::AdminResult(format!("admin operation {operation_id} {detail}"))
}

fn resource_type(resource: &testlab_schema::BrokerAclResource) -> AclResourceType {
    match resource {
        testlab_schema::BrokerAclResource::Topic { .. } => AclResourceType::TOPIC,
        testlab_schema::BrokerAclResource::Group { .. } => AclResourceType::GROUP,
        testlab_schema::BrokerAclResource::TransactionalId { .. } => {
            AclResourceType::TRANSACTIONAL_ID
        }
    }
}

fn schema_resource(
    value: AclResourceType,
    name: &str,
    operation_id: &OperationId,
) -> Result<testlab_schema::BrokerAclResource, AdapterError> {
    if value == AclResourceType::TOPIC {
        Ok(testlab_schema::BrokerAclResource::Topic {
            name: name.to_owned(),
        })
    } else if value == AclResourceType::GROUP {
        Ok(testlab_schema::BrokerAclResource::Group {
            name: name.to_owned(),
        })
    } else if value == AclResourceType::TRANSACTIONAL_ID {
        Ok(testlab_schema::BrokerAclResource::TransactionalId {
            name: name.to_owned(),
        })
    } else {
        Err(invalid_result(
            operation_id,
            "returned an unsupported ACL resource type",
        ))
    }
}

fn operation(value: testlab_schema::BrokerAclOperation) -> AclOperation {
    match value {
        testlab_schema::BrokerAclOperation::Read => AclOperation::READ,
        testlab_schema::BrokerAclOperation::Write => AclOperation::WRITE,
        testlab_schema::BrokerAclOperation::Create => AclOperation::CREATE,
    }
}

fn schema_operation(
    value: AclOperation,
    operation_id: &OperationId,
) -> Result<testlab_schema::BrokerAclOperation, AdapterError> {
    if value == AclOperation::READ {
        Ok(testlab_schema::BrokerAclOperation::Read)
    } else if value == AclOperation::WRITE {
        Ok(testlab_schema::BrokerAclOperation::Write)
    } else if value == AclOperation::CREATE {
        Ok(testlab_schema::BrokerAclOperation::Create)
    } else {
        Err(invalid_result(
            operation_id,
            "returned an unsupported ACL operation",
        ))
    }
}

fn permission(value: AclPermission) -> AclPermissionType {
    match value {
        AclPermission::Allow => AclPermissionType::ALLOW,
        AclPermission::Deny => AclPermissionType::DENY,
    }
}

fn schema_permission(
    value: AclPermissionType,
    operation_id: &OperationId,
) -> Result<AclPermission, AdapterError> {
    if value == AclPermissionType::ALLOW {
        Ok(AclPermission::Allow)
    } else if value == AclPermissionType::DENY {
        Ok(AclPermission::Deny)
    } else {
        Err(invalid_result(
            operation_id,
            "returned an unsupported ACL permission",
        ))
    }
}
