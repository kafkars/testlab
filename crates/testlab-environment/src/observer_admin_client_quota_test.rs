//! Client-quota observer tests pin correlation, CLI parsing, and retained raw truth.

use testlab_schema::{
    AdapterCommand, AlterClientQuotaAction, AlterClientQuotaCommand, BrokerQuotaDirection,
    BrokerStateObservation, ClientId, DescribeClientQuotaAction, OperationId, ScenarioAction,
};

use crate::observer_admin_target::{AdminTarget, ClientQuotaTarget};

#[test]
fn action_and_wire_command_map_to_one_exact_quota_target() {
    let action = ScenarioAction::DescribeClientQuota(describe());
    let command = AdapterCommand::DescribeClientQuota(testlab_schema::DescribeClientQuotaCommand {
        client_id: client(),
        operation_id: operation("quota-describe"),
        user: "testlab-user".to_owned(),
        direction: BrokerQuotaDirection::Producer,
        strict: false,
        timeout_ms: 1_000,
    });
    let target = AdminTarget::from_exact(&action, &command)
        .unwrap_or_else(|error| panic!("client-quota target: {error}"))
        .unwrap_or_else(|| panic!("missing client-quota target"));
    assert_eq!(
        target,
        AdminTarget::ClientQuota(ClientQuotaTarget {
            operation_id: operation("quota-describe"),
            user: "testlab-user".to_owned(),
            direction: BrokerQuotaDirection::Producer,
        })
    );

    let AdapterCommand::DescribeClientQuota(mut changed) = command else {
        panic!("client-quota command kind");
    };
    changed.strict = true;
    assert!(
        AdminTarget::from_exact(&action, &AdapterCommand::DescribeClientQuota(changed)).is_err()
    );

    let action = ScenarioAction::AlterClientQuota(alter());
    let command = AdapterCommand::AlterClientQuota(alter_command());
    assert!(matches!(
        AdminTarget::from_exact(&action, &command),
        Ok(Some(AdminTarget::ClientQuota(_)))
    ));
}

#[test]
fn cli_normalization_distinguishes_integral_presence_from_absence() {
    let present = crate::client_quota_cli_observation::normalize(
        7,
        &operation("quota-set"),
        "testlab-user",
        BrokerQuotaDirection::Producer,
        b"Quota configs for user-principal 'testlab-user' are producer_byte_rate=65536.0\n",
    )
    .unwrap_or_else(|error| panic!("present client quota: {error}"));
    let absent = crate::client_quota_cli_observation::normalize(
        8,
        &operation("quota-remove"),
        "testlab-user",
        BrokerQuotaDirection::Producer,
        b"Quota configs for user-principal 'testlab-user' are\n",
    )
    .unwrap_or_else(|error| panic!("absent client quota: {error}"));

    assert_quota(present, 7, "quota-set", Some(65_536));
    assert_quota(absent, 8, "quota-remove", None);
}

#[test]
fn cli_normalization_rejects_wrong_ambiguous_or_lossy_output() {
    for output in [
        "",
        "Quota configs for user-principal 'other' are producer_byte_rate=65536",
        "Quota configs for user-principal 'testlab-user' are consumer_byte_rate=65536",
        "Quota configs for user-principal 'testlab-user' are producer_byte_rate=65536, consumer_byte_rate=65536",
        "Quota configs for user-principal 'testlab-user' are producer_byte_rate=65536.5",
        "Quota configs for user-principal 'testlab-user' are producer_byte_rate=0",
        "Quota configs for user-principal 'testlab-user' are producer_byte_rate=65536\nunexpected",
    ] {
        assert!(
            crate::client_quota_cli_observation::normalize(
                0,
                &operation("quota"),
                "testlab-user",
                BrokerQuotaDirection::Producer,
                output.as_bytes(),
            )
            .is_err(),
            "{output:?}"
        );
    }
    assert!(
        crate::client_quota_cli_observation::normalize(
            0,
            &operation("quota"),
            "testlab-user",
            BrokerQuotaDirection::Producer,
            &[255],
        )
        .is_err()
    );
}

#[test]
fn cli_selection_and_query_terminal_remain_exact_and_raw() {
    assert_eq!(
        crate::client_quota_cli_observation::selection("testlab-user"),
        ["--entity-type", "users", "--entity-name", "testlab-user"]
    );
    let target = AdminTarget::ClientQuota(ClientQuotaTarget {
        operation_id: operation("quota-set"),
        user: "testlab-user".to_owned(),
        direction: BrokerQuotaDirection::Producer,
    });
    let fixture = crate::compose_test_fixture::Fixture::new(false);
    let mut environment = fixture.environment();
    environment.program = "/bin/sh".into();
    environment.prefix = vec![
        "-c".to_owned(),
        "printf '%s\\n' \"Quota configs for user-principal 'testlab-user' are producer_byte_rate=65536.0\""
            .to_owned(),
    ];

    let observed =
        environment.observe_client_quota_with_cli(&target, std::time::Duration::from_secs(2));
    assert!(observed.phase.succeeded(), "{:?}", observed.phase.failure);
    assert_eq!(observed.phase.operations.len(), 1);
    assert_eq!(observed.phase.artifacts.len(), 2);
    assert_eq!(observed.state_observations.len(), 1);

    let duplicate =
        environment.observe_client_quota_with_cli(&target, std::time::Duration::from_secs(2));
    assert!(!duplicate.phase.succeeded());
    assert!(duplicate.phase.operations.is_empty());
}

fn assert_quota(
    observation: BrokerStateObservation,
    ordinal: u64,
    operation_id: &str,
    rate: Option<u64>,
) {
    let BrokerStateObservation::ClientQuota(value) = observation else {
        panic!("client-quota observation kind");
    };
    assert_eq!(value.observation, ordinal);
    assert_eq!(value.operation_id, operation(operation_id));
    assert_eq!(value.user, "testlab-user");
    assert_eq!(value.direction, BrokerQuotaDirection::Producer);
    assert_eq!(value.bytes_per_second, rate);
}

fn describe() -> DescribeClientQuotaAction {
    DescribeClientQuotaAction {
        client_id: client(),
        operation_id: operation("quota-describe"),
        user: "testlab-user".to_owned(),
        direction: BrokerQuotaDirection::Producer,
        strict: false,
        expected_bytes_per_second: 65_536,
        timeout_ms: 1_000,
    }
}

fn alter() -> AlterClientQuotaAction {
    AlterClientQuotaAction {
        client_id: client(),
        operation_id: operation("quota-alter"),
        user: "testlab-user".to_owned(),
        direction: BrokerQuotaDirection::Producer,
        bytes_per_second: Some(65_536),
        validate_only: false,
        expected_current_bytes_per_second: None,
        timeout_ms: 1_000,
    }
}

fn alter_command() -> AlterClientQuotaCommand {
    AlterClientQuotaCommand {
        client_id: client(),
        operation_id: operation("quota-alter"),
        user: "testlab-user".to_owned(),
        direction: BrokerQuotaDirection::Producer,
        bytes_per_second: Some(65_536),
        validate_only: false,
        timeout_ms: 1_000,
    }
}

fn client() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client: {error}"))
}

fn operation(value: &str) -> OperationId {
    OperationId::new(value).unwrap_or_else(|error| panic!("operation: {error}"))
}
