//! User SCRAM protocol tests pin secret separation and exact event identity.

use testlab_schema::{
    AdapterCommand, AdapterEvent, AdminUserScramCredentialAlteration,
    AdminUserScramCredentialDescription, AlterUserScramCredentialAction, ClientId,
    DescribeUserScramCredentialAction, OperationId, ScenarioAction, ScramCredentialMechanism,
};

use crate::runner_protocol::{EventDisposition, ExpectedEvent};

#[test]
fn describe_translation_omits_the_verifier_owned_expected_iterations() {
    let action = ScenarioAction::DescribeUserScramCredential(describe());
    let Some((AdapterCommand::DescribeUserScramCredential(command), expected)) =
        crate::session_command_admin_user_scram::translate(&action)
    else {
        panic!("user SCRAM description translation");
    };

    assert_eq!(command.client_id, client());
    assert_eq!(command.operation_id, operation("scram-describe"));
    assert_eq!(command.user, "testlab-user");
    assert_eq!(command.mechanism, ScramCredentialMechanism::Sha256);
    assert_eq!(command.timeout_ms, 1_000);
    assert!(matches!(
        expected,
        ExpectedEvent::UserScramCredentialDescribed(_)
    ));
}

#[test]
fn alteration_translation_contains_no_password_or_environment_reference() {
    let action = ScenarioAction::AlterUserScramCredential(alter());
    let Some((command, expected)) = crate::session_command_admin_user_scram::translate(&action)
    else {
        panic!("user SCRAM alteration translation");
    };
    assert_eq!(command, AdapterCommand::AlterUserScramCredential(alter()));
    let encoded = serde_json::to_string(&command)
        .unwrap_or_else(|error| panic!("encode user SCRAM command: {error}"));
    assert!(!encoded.contains("password"));
    assert!(!encoded.contains("environment"));
    assert!(matches!(
        expected,
        ExpectedEvent::UserScramCredentialAltered(_)
    ));
}

#[test]
fn user_scram_completions_reject_foreign_identity_and_family() {
    let expected = [
        ExpectedEvent::UserScramCredentialAltered(operation("scram-operation")),
        ExpectedEvent::UserScramCredentialDescribed(operation("scram-operation")),
    ];
    let events = events("scram-operation");
    for (expected_index, expected) in expected.iter().enumerate() {
        for (event_index, event) in events.iter().enumerate() {
            let result = expected.classify(event);
            if expected_index == event_index {
                assert_eq!(
                    result.unwrap_or_else(|error| panic!("user SCRAM classification: {error}")),
                    EventDisposition::Complete
                );
            } else {
                let error = match result {
                    Ok(disposition) => {
                        panic!("cross-family user SCRAM event completed as {disposition:?}")
                    }
                    Err(error) => error,
                };
                assert_eq!(error.harness_error().code, "event_identity_mismatch");
            }
        }
    }
    for (expected, event) in expected.iter().zip(events("other-operation")) {
        let error = match expected.classify(&event) {
            Ok(disposition) => panic!("foreign user SCRAM identity completed as {disposition:?}"),
            Err(error) => error,
        };
        assert_eq!(error.harness_error().code, "event_identity_mismatch");
    }
}

fn events(operation_id: &str) -> [AdapterEvent; 2] {
    [
        AdapterEvent::UserScramCredentialAltered(AdminUserScramCredentialAlteration {
            operation_id: operation(operation_id),
            user: "testlab-user".to_owned(),
            mechanism: ScramCredentialMechanism::Sha256,
        }),
        AdapterEvent::UserScramCredentialDescribed(AdminUserScramCredentialDescription {
            operation_id: operation(operation_id),
            user: "testlab-user".to_owned(),
            mechanism: ScramCredentialMechanism::Sha256,
            iterations: Some(8_192),
            absence_broker_code: None,
        }),
    ]
}

fn alter() -> AlterUserScramCredentialAction {
    AlterUserScramCredentialAction {
        client_id: client(),
        operation_id: operation("scram-alter"),
        user: "testlab-user".to_owned(),
        mechanism: ScramCredentialMechanism::Sha256,
        iterations: Some(8_192),
        timeout_ms: 1_000,
    }
}

fn describe() -> DescribeUserScramCredentialAction {
    DescribeUserScramCredentialAction {
        client_id: client(),
        operation_id: operation("scram-describe"),
        user: "testlab-user".to_owned(),
        mechanism: ScramCredentialMechanism::Sha256,
        expected_iterations: Some(8_192),
        timeout_ms: 1_000,
    }
}

fn client() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client: {error}"))
}

fn operation(value: &str) -> OperationId {
    OperationId::new(value).unwrap_or_else(|error| panic!("operation: {error}"))
}
