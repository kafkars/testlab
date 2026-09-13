//! Delegation-token verification requires four public results and final CLI absence.

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
                "admin operation {} expected exact non-secret create, describe, renew, and expire results followed by immediate independent owner-filtered token absence",
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
mod tests {
    use std::collections::BTreeSet;

    use testlab_schema::{
        AdapterCommand, AdapterEvent, AdminDelegationTokenLifecycle, BrokerDelegationTokensState,
        BrokerStateObservation, Capability, ClientId, DelegationTokenPrincipalSpec,
        ExerciseDelegationTokenLifecycleAction, HistoryEntry, HistoryPayload, OperationId,
        SCENARIO_SCHEMA_VERSION, Scenario, ScenarioAction, ScenarioId,
    };

    use crate::admin::verify_admin;
    use crate::index::HistoryIndex;
    use crate::verify_fixture::{command, event, step};

    #[test]
    fn explicit_zero_delay_and_immediate_absence_pass() {
        assert!(violations(&history(Some(0), 0)).is_empty());
    }

    #[test]
    fn omitted_wire_delay_or_live_token_fails_explicit_expiration_contract() {
        assert_contract(&violations(&history(None, 0)));
        assert_contract(&violations(&history(Some(0), 1)));
    }

    fn history(command_expire_after_ms: Option<u64>, token_count: u32) -> Vec<HistoryEntry> {
        vec![
            command(
                1,
                AdapterCommand::ExerciseDelegationTokenLifecycle(action(command_expire_after_ms)),
            ),
            event(
                2,
                AdapterEvent::DelegationTokenLifecycleExercised(completion()),
            ),
            HistoryEntry {
                sequence: 3,
                observed_unix_ms: 3,
                payload: HistoryPayload::BrokerStateObservation {
                    observation: BrokerStateObservation::DelegationTokens(
                        BrokerDelegationTokensState {
                            observation: 0,
                            operation_id: operation(),
                            owner: principal(),
                            token_count,
                        },
                    ),
                },
            },
        ]
    }

    fn scenario() -> Scenario {
        Scenario {
            schema_version: SCENARIO_SCHEMA_VERSION,
            id: ScenarioId::new("delegation-token-expiry-test")
                .unwrap_or_else(|error| panic!("scenario: {error}")),
            title: "delegation token expiry".to_owned(),
            description: "delegation token expiry".to_owned(),
            timeout_ms: 30_000,
            requires: BTreeSet::from([Capability::Admin]),
            steps: vec![step(
                "delegation-token",
                ScenarioAction::ExerciseDelegationTokenLifecycle(action(Some(0))),
            )],
            assertions: Vec::new(),
        }
    }

    fn action(expire_after_ms: Option<u64>) -> ExerciseDelegationTokenLifecycleAction {
        ExerciseDelegationTokenLifecycleAction {
            client_id: ClientId::new("client-1").unwrap_or_else(|error| panic!("client: {error}")),
            operation_id: operation(),
            owner: principal(),
            renewers: vec![principal()],
            max_lifetime_ms: 604_800_000,
            renew_period_ms: 172_800_000,
            expire_after_ms,
            timeout_ms: 30_000,
        }
    }

    fn completion() -> AdminDelegationTokenLifecycle {
        AdminDelegationTokenLifecycle {
            operation_id: operation(),
            owner: principal(),
            requester: Some(principal()),
            renewers: vec![principal()],
            token_id: "token-1".to_owned(),
            issue_timestamp_ms: 1_700_000_000_000,
            initial_expiry_timestamp_ms: 1_700_086_400_000,
            max_timestamp_ms: 1_700_604_800_000,
            renewed_expiry_timestamp_ms: 1_700_172_800_010,
            expired_at_timestamp_ms: 1_700_000_000_020,
            hmac_size: 64,
            description_matched: true,
            create_throttle_time_ms: 0,
            describe_throttle_time_ms: 0,
            renew_throttle_time_ms: 0,
            expire_throttle_time_ms: 0,
        }
    }

    fn principal() -> DelegationTokenPrincipalSpec {
        DelegationTokenPrincipalSpec {
            principal_type: "User".to_owned(),
            principal_name: "kafkars".to_owned(),
        }
    }

    fn operation() -> OperationId {
        OperationId::new("delegation-token-lifecycle")
            .unwrap_or_else(|error| panic!("operation: {error}"))
    }

    fn violations(entries: &[HistoryEntry]) -> Vec<testlab_schema::Violation> {
        let index = HistoryIndex::build(entries);
        let mut violations = Vec::new();
        verify_admin(&scenario(), &index, &[], &mut violations);
        violations
    }

    fn assert_contract(violations: &[testlab_schema::Violation]) {
        assert!(
            violations
                .iter()
                .any(|violation| violation.contract_id.as_str() == "ADMIN-087"),
            "{violations:?}"
        );
    }
}
