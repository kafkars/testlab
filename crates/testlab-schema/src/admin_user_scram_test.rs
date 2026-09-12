//! User SCRAM schema tests pin secret separation and exact non-secret metadata.

use std::collections::{BTreeMap, BTreeSet};

use super::{
    AdapterCommand, AdapterEvent, AdminUserScramCredentialAlteration,
    AdminUserScramCredentialDescription, AlterUserScramCredentialAction, BrokerStateObservation,
    BrokerUserScramCredentialState, ClientId, DescribeUserScramCredentialAction,
    DescribeUserScramCredentialCommand, EVIDENCE_SCHEMA_VERSION, OperationId, PROTOCOL_VERSION,
    SASL_PASSWORD_ENVIRONMENT, SCENARIO_SCHEMA_VERSION, ScenarioAction, ScramCredentialMechanism,
};

#[test]
fn user_scram_cut_advances_all_versioned_boundaries() {
    assert_eq!(PROTOCOL_VERSION, 90);
    assert_eq!(SCENARIO_SCHEMA_VERSION, 93);
    assert_eq!(EVIDENCE_SCHEMA_VERSION, 79);
}

#[test]
fn actions_commands_public_results_and_broker_facts_round_trip_without_secrets() {
    for action in [
        ScenarioAction::AlterUserScramCredential(alter(Some(8_192))),
        ScenarioAction::AlterUserScramCredential(alter(None)),
        ScenarioAction::DescribeUserScramCredential(describe(Some(8_192))),
        ScenarioAction::DescribeUserScramCredential(describe(None)),
    ] {
        let encoded = toml::to_string(&action)
            .unwrap_or_else(|error| panic!("encode user SCRAM action: {error}"));
        assert!(!encoded.contains("kafkars-testlab-password"));
        assert!(!encoded.contains(SASL_PASSWORD_ENVIRONMENT));
        let decoded = toml::from_str::<ScenarioAction>(&encoded)
            .unwrap_or_else(|error| panic!("decode user SCRAM action: {error}"));
        assert_eq!(decoded, action);
    }

    round_trip(&AdapterCommand::AlterUserScramCredential(alter(Some(
        8_192,
    ))));
    round_trip(&AdapterCommand::DescribeUserScramCredential(
        describe_command(),
    ));
    round_trip(&AdapterEvent::UserScramCredentialAltered(
        AdminUserScramCredentialAlteration {
            operation_id: operation("scram-alter"),
            user: "testlab-user".to_owned(),
            mechanism: ScramCredentialMechanism::Sha256,
        },
    ));
    round_trip(&AdapterEvent::UserScramCredentialDescribed(
        AdminUserScramCredentialDescription {
            operation_id: operation("scram-describe"),
            user: "testlab-user".to_owned(),
            mechanism: ScramCredentialMechanism::Sha256,
            iterations: None,
            absence_broker_code: Some(91),
        },
    ));
    round_trip(&BrokerStateObservation::UserScramCredential(
        BrokerUserScramCredentialState {
            observation: 4,
            operation_id: operation("scram-alter"),
            user: "testlab-user".to_owned(),
            mechanism: ScramCredentialMechanism::Sha256,
            iterations: Some(8_192),
        },
    ));
}

#[test]
fn validation_accepts_deletion_and_absence_and_rejects_bad_iterations_or_users() {
    let clients = BTreeMap::from([(client(), false)]);
    let mut operation_ids = BTreeSet::new();
    let mut problems = Vec::new();
    for action in [
        ScenarioAction::AlterUserScramCredential(alter(None)),
        ScenarioAction::DescribeUserScramCredential(describe(None)),
    ] {
        crate::admin_action_validation::validate(
            &action,
            &clients,
            &mut operation_ids,
            &mut problems,
        );
    }
    assert!(problems.is_empty(), "{problems:?}");

    for (operation_id, user, iterations) in [
        ("scram-low", "testlab-user", Some(4_095)),
        ("scram-high", "testlab-user", Some(16_385)),
        ("scram-unsafe", "bad user", Some(8_192)),
    ] {
        let mut action = alter(iterations);
        action.operation_id = operation(operation_id);
        action.user = user.to_owned();
        crate::admin_action_validation::validate(
            &ScenarioAction::AlterUserScramCredential(action),
            &clients,
            &mut operation_ids,
            &mut problems,
        );
    }
    for expected in ["iterations must be between", "invalid SCRAM user"] {
        assert!(
            problems.iter().any(|problem| problem.contains(expected)),
            "missing {expected:?} in {problems:?}"
        );
    }
}

fn alter(iterations: Option<u32>) -> AlterUserScramCredentialAction {
    AlterUserScramCredentialAction {
        client_id: client(),
        operation_id: operation("scram-alter"),
        user: "testlab-user".to_owned(),
        mechanism: ScramCredentialMechanism::Sha256,
        iterations,
        timeout_ms: 1_000,
    }
}

fn describe(expected_iterations: Option<u32>) -> DescribeUserScramCredentialAction {
    DescribeUserScramCredentialAction {
        client_id: client(),
        operation_id: operation("scram-describe"),
        user: "testlab-user".to_owned(),
        mechanism: ScramCredentialMechanism::Sha256,
        expected_iterations,
        timeout_ms: 1_000,
    }
}

fn describe_command() -> DescribeUserScramCredentialCommand {
    DescribeUserScramCredentialCommand {
        client_id: client(),
        operation_id: operation("scram-describe"),
        user: "testlab-user".to_owned(),
        mechanism: ScramCredentialMechanism::Sha256,
        timeout_ms: 1_000,
    }
}

fn round_trip<T>(value: &T)
where
    T: serde::Serialize + serde::de::DeserializeOwned + std::fmt::Debug + PartialEq,
{
    let encoded = serde_json::to_string(value)
        .unwrap_or_else(|error| panic!("encode user SCRAM value: {error}"));
    let decoded = serde_json::from_str::<T>(&encoded)
        .unwrap_or_else(|error| panic!("decode user SCRAM value: {error}"));
    assert_eq!(&decoded, value);
}

fn client() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client: {error}"))
}

fn operation(value: &str) -> OperationId {
    OperationId::new(value).unwrap_or_else(|error| panic!("operation: {error}"))
}
