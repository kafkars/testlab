//! Pinned Kafka ACL CLI output becomes one exact literal wildcard-host state fact.

use std::collections::BTreeMap;

use testlab_schema::{
    AclPermission, BrokerAclState, BrokerStateObservation, LiteralAclBinding, OperationId,
};

use crate::observer_error::ObserverError;

pub(super) fn selection(binding: &LiteralAclBinding) -> Vec<String> {
    let flag = match &binding.resource {
        testlab_schema::BrokerAclResource::Topic { .. } => "--topic",
        testlab_schema::BrokerAclResource::Group { .. } => "--group",
        testlab_schema::BrokerAclResource::TransactionalId { .. } => "--transactional-id",
    };
    vec![flag.to_owned(), binding.resource.name().to_owned()]
}

pub(super) fn normalize(
    observation: u64,
    operation_id: &OperationId,
    binding: &LiteralAclBinding,
    stdout: &[u8],
) -> Result<BrokerStateObservation, ObserverError> {
    let output = std::str::from_utf8(stdout).map_err(|_| invalid("output is not UTF-8"))?;
    let rows = parse(binding, output)?;
    Ok(BrokerStateObservation::Acl(BrokerAclState {
        observation,
        operation_id: operation_id.clone(),
        binding: binding.clone(),
        present: rows.iter().any(|row| row == binding),
    }))
}

fn parse(
    selected: &LiteralAclBinding,
    output: &str,
) -> Result<Vec<LiteralAclBinding>, ObserverError> {
    if output.trim().is_empty() {
        return Ok(Vec::new());
    }
    let header = format!(
        "resourceType={}, name={}, patternType=LITERAL",
        resource_type(&selected.resource),
        selected.resource.name()
    );
    let mut header_seen = false;
    let mut rows = Vec::new();
    for line in output
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
    {
        if line.starts_with("Current ACLs for resource") {
            if header_seen || !line.contains(&header) {
                return Err(invalid(
                    "reported an unexpected or duplicate resource header",
                ));
            }
            header_seen = true;
        } else if line.starts_with("(principal=") {
            if !header_seen {
                return Err(invalid("reported an ACL before its resource header"));
            }
            rows.push(parse_row(selected, line)?);
        } else {
            return Err(invalid("reported an unexpected output row"));
        }
    }
    if !header_seen {
        return Err(invalid("omitted the exact literal resource header"));
    }
    Ok(rows)
}

fn parse_row(selected: &LiteralAclBinding, line: &str) -> Result<LiteralAclBinding, ObserverError> {
    let body = line
        .strip_prefix('(')
        .and_then(|value| value.strip_suffix(')'))
        .ok_or_else(|| invalid("reported a malformed ACL row"))?;
    let mut fields = BTreeMap::new();
    for field in body.split(',').map(str::trim) {
        let (name, value) = field
            .split_once('=')
            .ok_or_else(|| invalid("reported a malformed ACL field"))?;
        if fields.insert(name, value).is_some() {
            return Err(invalid("reported a duplicate ACL field"));
        }
    }
    if fields.len() != 4 || fields.get("host") != Some(&"*") {
        return Err(invalid("reported an unsupported ACL row shape"));
    }
    Ok(LiteralAclBinding {
        resource: selected.resource.clone(),
        principal: required(&fields, "principal")?.to_owned(),
        operation: match required(&fields, "operation")? {
            "READ" => testlab_schema::BrokerAclOperation::Read,
            "WRITE" => testlab_schema::BrokerAclOperation::Write,
            "CREATE" => testlab_schema::BrokerAclOperation::Create,
            _ => return Err(invalid("reported an unsupported ACL operation")),
        },
        permission: match required(&fields, "permissionType")? {
            "ALLOW" => AclPermission::Allow,
            "DENY" => AclPermission::Deny,
            _ => return Err(invalid("reported an unsupported ACL permission")),
        },
    })
}

fn required<'a>(fields: &'a BTreeMap<&str, &str>, name: &str) -> Result<&'a str, ObserverError> {
    fields
        .get(name)
        .copied()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| invalid("reported a missing ACL field"))
}

fn resource_type(resource: &testlab_schema::BrokerAclResource) -> &'static str {
    match resource {
        testlab_schema::BrokerAclResource::Topic { .. } => "TOPIC",
        testlab_schema::BrokerAclResource::Group { .. } => "GROUP",
        testlab_schema::BrokerAclResource::TransactionalId { .. } => "TRANSACTIONAL_ID",
    }
}

fn invalid(message: &str) -> ObserverError {
    ObserverError::InvalidBrokerState(format!("Kafka CLI ACL snapshot: {message}"))
}
