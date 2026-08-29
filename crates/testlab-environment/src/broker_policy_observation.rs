//! Broker-policy query parsing rejects unknown output instead of inferring state.

use testlab_schema::{BrokerAclResource, BrokerPolicy};

pub(super) fn parse(policy: &BrokerPolicy, stdout: &[u8]) -> Result<bool, String> {
    let output = std::str::from_utf8(stdout)
        .map_err(|error| format!("broker policy query output was not UTF-8: {error}"))?;
    match policy {
        BrokerPolicy::Acl {
            resource,
            operation,
        } => parse_acl(resource, operation.cli_name(), output),
        BrokerPolicy::Quota {
            direction,
            bytes_per_second,
            ..
        } => parse_quota(direction.config_name(), *bytes_per_second, output),
    }
}

fn parse_acl(resource: &BrokerAclResource, operation: &str, output: &str) -> Result<bool, String> {
    if output.trim().is_empty() {
        return Err("broker ACL query output was empty".to_owned());
    }
    let resource_type = match resource {
        BrokerAclResource::Topic { .. } => "TOPIC",
        BrokerAclResource::Group { .. } => "GROUP",
        BrokerAclResource::TransactionalId { .. } => "TRANSACTIONAL_ID",
    };
    let header = [
        format!("resourceType={resource_type}"),
        format!("name={},", resource.name()),
        "patternType=LITERAL".to_owned(),
    ];
    if !header.iter().all(|token| output.contains(token)) {
        return Err("broker ACL query output omitted the exact literal resource".to_owned());
    }
    let operation = format!("operation={}", operation.to_ascii_uppercase());
    Ok(output.lines().any(|line| {
        [
            "principal=User:kafkars",
            operation.as_str(),
            "permissionType=DENY",
        ]
        .iter()
        .all(|token| line.contains(token))
    }))
}

fn parse_quota(key: &str, expected: u64, output: &str) -> Result<bool, String> {
    if !output.contains("Quota configs for user-principal 'kafkars'") {
        return Err("broker quota query output omitted the exact user principal".to_owned());
    }
    let Some(tail) = output.split_once(&format!("{key}=")).map(|(_, tail)| tail) else {
        return Ok(false);
    };
    let token = tail
        .split(|character: char| character == ',' || character.is_ascii_whitespace())
        .next()
        .unwrap_or_default();
    let (whole, fraction) = match token.split_once('.') {
        Some((whole, fraction)) if !fraction.is_empty() => (whole, Some(fraction)),
        Some(_) => return Err("broker quota value had an empty decimal fraction".to_owned()),
        None => (token, None),
    };
    let value = whole
        .parse::<u64>()
        .map_err(|error| format!("broker quota value was not numeric: {error}"))?;
    if value != expected
        || fraction.is_some_and(|digits| digits.chars().any(|digit| digit != '0'))
        || token.matches('.').count() > 1
    {
        return Err(format!(
            "broker quota query reported {key}={token}, expected {expected}"
        ));
    }
    Ok(true)
}
