//! User SCRAM observer tests pin correlation and strict CLI normalization.

use testlab_schema::{
    AdapterCommand, AlterUserScramCredentialAction, BrokerStateObservation, ClientId,
    DescribeUserScramCredentialAction, DescribeUserScramCredentialCommand, OperationId,
    ScenarioAction, ScramCredentialMechanism,
};

use crate::observer_admin_target::{AdminTarget, UserScramCredentialTarget};

#[test]
fn action_and_wire_command_map_to_one_exact_scram_target() {
    let action = ScenarioAction::DescribeUserScramCredential(describe());
    let command = AdapterCommand::DescribeUserScramCredential(DescribeUserScramCredentialCommand {
        client_id: client(),
        operation_id: operation("scram-describe"),
        user: "testlab-user".to_owned(),
        mechanism: mechanism(),
        timeout_ms: 1_000,
    });
    let target = AdminTarget::from_exact(&action, &command)
        .unwrap_or_else(|error| panic!("user SCRAM target: {error}"))
        .unwrap_or_else(|| panic!("missing user SCRAM target"));
    assert_eq!(
        target,
        AdminTarget::UserScramCredential(UserScramCredentialTarget {
            operation_id: operation("scram-describe"),
            user: "testlab-user".to_owned(),
            mechanism: mechanism(),
        })
    );
    let AdapterCommand::DescribeUserScramCredential(mut changed) = command else {
        panic!("user SCRAM command kind");
    };
    changed.mechanism = ScramCredentialMechanism::Sha512;
    assert!(
        AdminTarget::from_exact(
            &action,
            &AdapterCommand::DescribeUserScramCredential(changed),
        )
        .is_err()
    );

    let alter = alter();
    assert!(matches!(
        AdminTarget::from_exact(
            &ScenarioAction::AlterUserScramCredential(alter.clone()),
            &AdapterCommand::AlterUserScramCredential(alter),
        ),
        Ok(Some(AdminTarget::UserScramCredential(_)))
    ));
}

#[test]
fn cli_normalization_distinguishes_exact_presence_from_absence() {
    let present = normalize(
        7,
        "scram-upsert",
        "Quota configs for user-principal 'testlab-user' are\nSCRAM credential configs for user-principal 'testlab-user' are SCRAM-SHA-256=iterations=8192\n",
    );
    let absent = normalize(
        8,
        "scram-delete",
        "Quota configs for user-principal 'testlab-user' are\nError retrieving SCRAM credential configs for user-principal 'testlab-user': ExecutionException: org.apache.kafka.common.errors.ResourceNotFoundException: No SCRAM credentials\n",
    );
    let empty = normalize(9, "scram-delete-empty", "");
    assert_scram(present, 7, "scram-upsert", Some(8_192));
    assert_scram(absent, 8, "scram-delete", None);
    assert_scram(empty, 9, "scram-delete-empty", None);
}

#[test]
fn cli_normalization_rejects_wrong_ambiguous_or_lossy_output() {
    for output in [
        "SCRAM credential configs for user-principal 'other' are SCRAM-SHA-256=iterations=8192",
        "SCRAM credential configs for user-principal 'testlab-user' are SCRAM-SHA-512=iterations=8192",
        "SCRAM credential configs for user-principal 'testlab-user' are SCRAM-SHA-256=iterations=8192,SCRAM-SHA-512=iterations=8192",
        "SCRAM credential configs for user-principal 'testlab-user' are SCRAM-SHA-256=iterations=4095",
        "Quota configs for user-principal 'testlab-user' are producer_byte_rate=65536\nSCRAM credential configs for user-principal 'testlab-user' are SCRAM-SHA-256=iterations=8192",
        "Quota configs for user-principal 'testlab-user' are\nunexpected",
    ] {
        assert!(
            crate::user_scram_cli_observation::normalize(
                0,
                &operation("scram"),
                "testlab-user",
                mechanism(),
                output.as_bytes(),
            )
            .is_err(),
            "{output:?}"
        );
    }
}

#[test]
fn cli_selection_and_query_terminal_remain_exact_and_raw() {
    assert_eq!(
        crate::user_scram_cli_observation::selection("testlab-user"),
        ["--entity-type", "users", "--entity-name", "testlab-user"]
    );
    let target = AdminTarget::UserScramCredential(UserScramCredentialTarget {
        operation_id: operation("scram-observe"),
        user: "testlab-user".to_owned(),
        mechanism: mechanism(),
    });
    let fixture = crate::compose_test_fixture::Fixture::new(false);
    let mut environment = fixture.environment();
    environment.program = "/bin/sh".into();
    environment.prefix = vec![
        "-c".to_owned(),
        "printf '%s\\n' \"Quota configs for user-principal 'testlab-user' are\" \"SCRAM credential configs for user-principal 'testlab-user' are SCRAM-SHA-256=iterations=8192\""
            .to_owned(),
    ];

    let observed =
        environment.observe_user_scram_with_cli(&target, std::time::Duration::from_secs(2));
    assert!(observed.phase.succeeded(), "{:?}", observed.phase.failure);
    assert_eq!(observed.phase.operations.len(), 1);
    assert_eq!(observed.phase.artifacts.len(), 2);
    assert_eq!(observed.state_observations.len(), 1);

    let duplicate =
        environment.observe_user_scram_with_cli(&target, std::time::Duration::from_secs(2));
    assert!(!duplicate.phase.succeeded());
    assert!(duplicate.phase.operations.is_empty());
}

fn normalize(observation: u64, operation_id: &str, output: &str) -> BrokerStateObservation {
    crate::user_scram_cli_observation::normalize(
        observation,
        &operation(operation_id),
        "testlab-user",
        mechanism(),
        output.as_bytes(),
    )
    .unwrap_or_else(|error| panic!("user SCRAM observation: {error}"))
}

fn assert_scram(
    observation: BrokerStateObservation,
    ordinal: u64,
    operation_id: &str,
    iterations: Option<u32>,
) {
    let BrokerStateObservation::UserScramCredential(value) = observation else {
        panic!("user SCRAM observation kind");
    };
    assert_eq!(value.observation, ordinal);
    assert_eq!(value.operation_id, operation(operation_id));
    assert_eq!(value.user, "testlab-user");
    assert_eq!(value.mechanism, mechanism());
    assert_eq!(value.iterations, iterations);
}

fn describe() -> DescribeUserScramCredentialAction {
    DescribeUserScramCredentialAction {
        client_id: client(),
        operation_id: operation("scram-describe"),
        user: "testlab-user".to_owned(),
        mechanism: mechanism(),
        expected_iterations: Some(8_192),
        timeout_ms: 1_000,
    }
}

fn alter() -> AlterUserScramCredentialAction {
    AlterUserScramCredentialAction {
        client_id: client(),
        operation_id: operation("scram-alter"),
        user: "testlab-user".to_owned(),
        mechanism: mechanism(),
        iterations: Some(8_192),
        timeout_ms: 1_000,
    }
}

const fn mechanism() -> ScramCredentialMechanism {
    ScramCredentialMechanism::Sha256
}

fn client() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client: {error}"))
}

fn operation(value: &str) -> OperationId {
    OperationId::new(value).unwrap_or_else(|error| panic!("operation: {error}"))
}
