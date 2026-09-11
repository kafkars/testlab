//! User SCRAM validation bounds identities, users, iterations, and deadlines.

use std::collections::{BTreeMap, BTreeSet};

use crate::admin_action_validation::{validate_identity, validate_timeout};
use crate::{ClientId, OperationId, ScenarioAction};

const MIN_ITERATIONS: u32 = 4_096;
const MAX_ITERATIONS: u32 = 16_384;

pub(crate) fn validate(
    action: &ScenarioAction,
    clients: &BTreeMap<ClientId, bool>,
    operation_ids: &mut BTreeSet<OperationId>,
    problems: &mut Vec<String>,
) -> bool {
    let (client_id, operation_id, user, iterations, timeout_ms) = match action {
        ScenarioAction::AlterUserScramCredential(value) => (
            &value.client_id,
            &value.operation_id,
            value.user.as_str(),
            value.iterations,
            value.timeout_ms,
        ),
        ScenarioAction::DescribeUserScramCredential(value) => (
            &value.client_id,
            &value.operation_id,
            value.user.as_str(),
            value.expected_iterations,
            value.timeout_ms,
        ),
        _ => return false,
    };
    validate_identity(client_id, operation_id, clients, operation_ids, problems);
    validate_timeout(operation_id, timeout_ms, problems);
    if !safe_user(user) {
        problems.push(format!(
            "admin operation {operation_id} has invalid SCRAM user"
        ));
    }
    if iterations.is_some_and(|value| !(MIN_ITERATIONS..=MAX_ITERATIONS).contains(&value)) {
        problems.push(format!(
            "admin operation {operation_id} SCRAM iterations must be between {MIN_ITERATIONS} and {MAX_ITERATIONS}"
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
