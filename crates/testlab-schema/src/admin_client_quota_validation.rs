//! Client-quota validation bounds exact named users, rates, identities, and deadlines.

use std::collections::{BTreeMap, BTreeSet};

use crate::admin_action_validation::{validate_identity, validate_timeout};
use crate::{ClientId, OperationId, ScenarioAction};

const MAX_RATE: u64 = 4_294_967_295;

pub(crate) fn validate(
    action: &ScenarioAction,
    clients: &BTreeMap<ClientId, bool>,
    operation_ids: &mut BTreeSet<OperationId>,
    problems: &mut Vec<String>,
) -> bool {
    let (client_id, operation_id, user, rate, timeout_ms) = match action {
        ScenarioAction::AlterClientQuota(value) => (
            &value.client_id,
            &value.operation_id,
            value.user.as_str(),
            value.bytes_per_second,
            value.timeout_ms,
        ),
        ScenarioAction::DescribeClientQuota(value) => (
            &value.client_id,
            &value.operation_id,
            value.user.as_str(),
            Some(value.expected_bytes_per_second),
            value.timeout_ms,
        ),
        _ => return false,
    };
    validate_identity(client_id, operation_id, clients, operation_ids, problems);
    validate_timeout(operation_id, timeout_ms, problems);
    if !safe_user(user) {
        problems.push(format!(
            "admin operation {operation_id} has invalid client-quota user"
        ));
    }
    if rate.is_some_and(|value| !(1..=MAX_RATE).contains(&value)) {
        problems.push(format!(
            "admin operation {operation_id} client-quota rate must be between 1 and {MAX_RATE}"
        ));
    }
    true
}

fn safe_user(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}
