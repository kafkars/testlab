//! ACL action validation bounds exact CLI-observable bindings and public calls.

use std::collections::{BTreeMap, BTreeSet};

use crate::admin_action_validation::{validate_identity, validate_timeout};
use crate::{ClientId, LiteralAclBinding, OperationId, ScenarioAction};

const MAX_BINDINGS: usize = 32;

pub(crate) fn validate(
    action: &ScenarioAction,
    clients: &BTreeMap<ClientId, bool>,
    operation_ids: &mut BTreeSet<OperationId>,
    problems: &mut Vec<String>,
) -> bool {
    let (client_id, operation_id, bindings, timeout_ms) = match action {
        ScenarioAction::CreateAcls(value) => (
            &value.client_id,
            &value.operation_id,
            value.bindings.as_slice(),
            value.timeout_ms,
        ),
        ScenarioAction::DescribeAcls(value) => (
            &value.client_id,
            &value.operation_id,
            std::slice::from_ref(&value.binding),
            value.timeout_ms,
        ),
        ScenarioAction::DeleteAcls(value) => (
            &value.client_id,
            &value.operation_id,
            value.bindings.as_slice(),
            value.timeout_ms,
        ),
        _ => return false,
    };
    validate_identity(client_id, operation_id, clients, operation_ids, problems);
    validate_timeout(operation_id, timeout_ms, problems);
    validate_bindings(operation_id, bindings, problems);
    true
}

fn validate_bindings(
    operation_id: &OperationId,
    bindings: &[LiteralAclBinding],
    problems: &mut Vec<String>,
) {
    if bindings.is_empty() || bindings.len() > MAX_BINDINGS {
        problems.push(format!(
            "admin operation {operation_id} ACL bindings must contain 1 to {MAX_BINDINGS} entries"
        ));
    }
    let mut unique = BTreeSet::new();
    for binding in bindings {
        let resource = binding.resource.name();
        if !safe_token(resource, 249) {
            problems.push(format!(
                "admin operation {operation_id} has invalid ACL resource"
            ));
        }
        let principal = binding.principal.strip_prefix("User:").unwrap_or_default();
        if !safe_token(principal, 128) {
            problems.push(format!(
                "admin operation {operation_id} has invalid ACL principal"
            ));
        }
        if !unique.insert(binding) {
            problems.push(format!(
                "admin operation {operation_id} ACL bindings must be unique"
            ));
        }
    }
}

fn safe_token(value: &str, maximum: usize) -> bool {
    !value.is_empty()
        && value.len() <= maximum
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}
