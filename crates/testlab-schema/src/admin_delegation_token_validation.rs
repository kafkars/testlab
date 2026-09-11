//! Delegation-token scenarios keep principal and lifetime requests bounded.

use std::collections::{BTreeMap, BTreeSet};

use crate::admin_action_validation::{validate_identity, validate_timeout};
use crate::{ClientId, OperationId, ScenarioAction};

const MAX_PRINCIPAL_BYTES: usize = 255;
const MAX_RENEWERS: usize = 32;
const MAX_LIFETIME_MS: u64 = 30 * 24 * 60 * 60 * 1_000;

pub(crate) fn validate(
    action: &ScenarioAction,
    clients: &BTreeMap<ClientId, bool>,
    operation_ids: &mut BTreeSet<OperationId>,
    problems: &mut Vec<String>,
) -> bool {
    let ScenarioAction::ExerciseDelegationTokenLifecycle(action) = action else {
        return false;
    };
    validate_identity(
        &action.client_id,
        &action.operation_id,
        clients,
        operation_ids,
        problems,
    );
    validate_timeout(&action.operation_id, action.timeout_ms, problems);
    validate_principal(&action.operation_id, "owner", &action.owner, problems);
    if action.renewers.is_empty() || action.renewers.len() > MAX_RENEWERS {
        problems.push(format!(
            "admin operation {} renewers must contain 1 to {MAX_RENEWERS} principals",
            action.operation_id
        ));
    }
    let mut unique = BTreeSet::new();
    for renewer in &action.renewers {
        validate_principal(&action.operation_id, "renewer", renewer, problems);
        if !unique.insert(renewer) {
            problems.push(format!(
                "admin operation {} repeats delegation-token renewer {}",
                action.operation_id,
                renewer.kafka_name()
            ));
        }
    }
    if !(1..=MAX_LIFETIME_MS).contains(&action.max_lifetime_ms) {
        problems.push(format!(
            "admin operation {} max_lifetime_ms must be between 1 and {MAX_LIFETIME_MS}",
            action.operation_id
        ));
    }
    if action.renew_period_ms == 0 || action.renew_period_ms >= action.max_lifetime_ms {
        problems.push(format!(
            "admin operation {} renew_period_ms must be positive and less than max_lifetime_ms",
            action.operation_id
        ));
    }
    true
}

fn validate_principal(
    operation_id: &OperationId,
    field: &str,
    principal: &crate::DelegationTokenPrincipalSpec,
    problems: &mut Vec<String>,
) {
    if invalid_component(&principal.principal_type) || invalid_component(&principal.principal_name)
    {
        problems.push(format!(
            "admin operation {operation_id} has invalid delegation-token {field}"
        ));
    }
}

fn invalid_component(value: &str) -> bool {
    value.is_empty()
        || value.len() > MAX_PRINCIPAL_BYTES
        || value
            .chars()
            .any(|character| character.is_control() || character == ':')
}
