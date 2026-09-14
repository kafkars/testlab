//! Delegation-token verification requires four public results and no final live CLI token.

use testlab_schema::{AdminDelegationTokenLifecycle, ScenarioAction, Violation};

use crate::admin::{immediate_after_public, public_after_command};
use crate::index::HistoryIndex;
use crate::index::admin_lifecycle::Indexed;
use crate::support::violation;

const MAX_HMAC_BYTES: usize = 64 * 1024;

pub(crate) fn verify(
    action: &ScenarioAction,
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) -> bool {
    let window = index.admin_command_window(action);
    let ScenarioAction::ExerciseDelegationTokenLifecycle(action) = action else {
        return false;
    };
    let contract = if action.expire_after_ms.is_some() {
        "ADMIN-087"
    } else {
        "ADMIN-073"
    };
    let public = one(index
        .admin_lifecycles
        .delegation_completed
        .get(&action.operation_id));
    let independent = one(index
        .admin_lifecycles
        .delegation_observed
        .get(&action.operation_id));
    let matches = public.is_some_and(|public| {
        independent.is_some_and(|independent| {
            public_matches(&public.value, action)
                && public_after_command(window, public.history_sequence)
                && independent.value.operation_id == action.operation_id
                && independent.value.owner == action.owner
                && independent.value.token_count == 0
                && immediate_after_public(
                    window,
                    public.history_sequence,
                    independent.history_sequence,
                )
        })
    });
    if !matches {
        violations.push(violation(
            contract,
            format!(
                "admin operation {} expected exact non-secret create, describe, renew, and expire results followed by an immediate independent owner-filtered live-token count of zero",
                action.operation_id
            ),
            Some(action.operation_id.clone()),
            public
                .map(|value| format!("history:{}", value.history_sequence))
                .into_iter()
                .chain(independent.map(|value| {
                    format!("broker-state-observation:{}", value.value.observation)
                }))
                .collect(),
        ));
    }
    true
}

fn public_matches(
    public: &AdminDelegationTokenLifecycle,
    action: &testlab_schema::ExerciseDelegationTokenLifecycleAction,
) -> bool {
    public.operation_id == action.operation_id
        && public.owner == action.owner
        && public.requester.as_ref() == Some(&action.owner)
        && public.renewers == action.renewers
        && valid_token_id(&public.token_id)
        && public.hmac_size > 0
        && public.hmac_size <= MAX_HMAC_BYTES
        && public.description_matched
        && valid_timestamps(public, action.max_lifetime_ms)
        && [
            public.create_throttle_time_ms,
            public.describe_throttle_time_ms,
            public.renew_throttle_time_ms,
            public.expire_throttle_time_ms,
        ]
        .into_iter()
        .all(|throttle| throttle <= action.timeout_ms)
}

fn valid_token_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 255
        && !value
            .chars()
            .any(|character| character.is_control() || character.is_whitespace())
}

fn valid_timestamps(public: &AdminDelegationTokenLifecycle, max_lifetime_ms: u64) -> bool {
    let Ok(max_lifetime_ms) = i64::try_from(max_lifetime_ms) else {
        return false;
    };
    public.issue_timestamp_ms >= 0
        && public.initial_expiry_timestamp_ms > public.issue_timestamp_ms
        && public.initial_expiry_timestamp_ms < public.max_timestamp_ms
        && public.max_timestamp_ms - public.issue_timestamp_ms == max_lifetime_ms
        && public.renewed_expiry_timestamp_ms > public.initial_expiry_timestamp_ms
        && public.renewed_expiry_timestamp_ms <= public.max_timestamp_ms
        && public.expired_at_timestamp_ms >= public.issue_timestamp_ms
        && public.expired_at_timestamp_ms < public.renewed_expiry_timestamp_ms
}

fn one<T>(values: Option<&Vec<Indexed<T>>>) -> Option<&Indexed<T>> {
    let [value] = values?.as_slice() else {
        return None;
    };
    Some(value)
}

#[cfg(test)]
#[path = "admin_delegation_token_test.rs"]
mod tests;
