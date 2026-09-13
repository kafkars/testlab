//! Delegation-token protocol tests keep lifecycle facts typed and secret-free.

use std::collections::{BTreeMap, BTreeSet};

use crate::*;

#[test]
fn lifecycle_payloads_round_trip_without_a_secret_field() {
    assert_eq!(PROTOCOL_VERSION, 135);
    assert_eq!(SCENARIO_SCHEMA_VERSION, 139);
    assert_eq!(EVIDENCE_SCHEMA_VERSION, 125);
    let lifecycle = action(Some(0));
    let encoded_action =
        serde_json::to_string(&lifecycle).unwrap_or_else(|error| panic!("encode action: {error}"));
    assert!(encoded_action.contains("\"expire_after_ms\":0"));
    round_trip(&ScenarioAction::ExerciseDelegationTokenLifecycle(
        lifecycle.clone(),
    ));
    round_trip(&AdapterCommand::ExerciseDelegationTokenLifecycle(lifecycle));
    round_trip(&ScenarioAction::ExerciseDelegationTokenLifecycle(action(
        None,
    )));
    let event = AdapterEvent::DelegationTokenLifecycleExercised(completion());
    let encoded = serde_json::to_string(&event).unwrap_or_else(|error| panic!("encode: {error}"));
    assert!(!encoded.contains("hmac_bytes"));
    assert!(!encoded.contains("secret-value"));
    round_trip(&event);
    round_trip(&BrokerStateObservation::DelegationTokens(
        BrokerDelegationTokensState {
            observation: 4,
            operation_id: operation(),
            owner: principal(),
            token_count: 0,
        },
    ));
}

#[test]
fn validation_limits_explicit_expiration_to_deterministic_zero_delay() {
    let mut operation_ids = BTreeSet::new();
    let mut problems = Vec::new();
    super::validation::validate(
        &ScenarioAction::ExerciseDelegationTokenLifecycle(action(Some(1))),
        &BTreeMap::from([(id("client-1"), false)]),
        &mut operation_ids,
        &mut problems,
    );
    assert!(
        problems
            .iter()
            .any(|problem| problem.contains("expire_after_ms must be zero")),
        "{problems:?}"
    );
}

fn action(expire_after_ms: Option<u64>) -> ExerciseDelegationTokenLifecycleAction {
    ExerciseDelegationTokenLifecycleAction {
        client_id: id("client-1"),
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
    OperationId::new("admin-delegation-token-lifecycle")
        .unwrap_or_else(|error| panic!("operation id: {error}"))
}

fn id(value: &str) -> ClientId {
    ClientId::new(value).unwrap_or_else(|error| panic!("client id: {error}"))
}

fn round_trip<T>(value: &T)
where
    T: serde::Serialize + serde::de::DeserializeOwned + Eq + std::fmt::Debug,
{
    let encoded = serde_json::to_vec(value).unwrap_or_else(|error| panic!("encode: {error}"));
    let decoded =
        serde_json::from_slice(&encoded).unwrap_or_else(|error| panic!("decode: {error}"));
    assert_eq!(value, &decoded);
}
