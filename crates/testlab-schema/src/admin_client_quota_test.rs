//! Client-quota schema tests pin expectation separation and bounded exact values.

use std::collections::{BTreeMap, BTreeSet};

use super::{
    AdapterCommand, AdapterEvent, AdminClientQuotaAlteration, AdminClientQuotaDescription,
    AlterClientQuotaAction, AlterClientQuotaCommand, BrokerClientQuotaState, BrokerQuotaDirection,
    BrokerStateObservation, Capability, ClientId, DescribeClientQuotaAction,
    DescribeClientQuotaCommand, EVIDENCE_SCHEMA_VERSION, OperationId, PROTOCOL_VERSION,
    SCENARIO_SCHEMA_VERSION, Scenario, ScenarioAction, ScenarioId, ScenarioStep, StepId,
};

#[test]
fn client_quota_cut_advances_all_versioned_boundaries() {
    assert_eq!(PROTOCOL_VERSION, 146);
    assert_eq!(SCENARIO_SCHEMA_VERSION, 150);
    assert_eq!(EVIDENCE_SCHEMA_VERSION, 136);
}

#[test]
fn actions_commands_public_results_and_broker_facts_round_trip() {
    for action in [
        ScenarioAction::AlterClientQuota(alter(Some(65_536))),
        ScenarioAction::AlterClientQuota(validated("quota-validate", 32_768, 65_536)),
        ScenarioAction::AlterClientQuota(alter(None)),
        ScenarioAction::DescribeClientQuota(describe()),
    ] {
        let encoded = toml::to_string(&action)
            .unwrap_or_else(|error| panic!("encode client quota action: {error}"));
        let decoded = toml::from_str::<ScenarioAction>(&encoded)
            .unwrap_or_else(|error| panic!("decode client quota action: {error}"));
        assert_eq!(decoded, action);
    }

    round_trip(&AdapterCommand::AlterClientQuota(alter_command(
        "quota-alter",
        Some(65_536),
        false,
    )));
    round_trip(&AdapterCommand::AlterClientQuota(alter_command(
        "quota-validate",
        Some(32_768),
        true,
    )));
    round_trip(&AdapterCommand::DescribeClientQuota(describe_command()));
    round_trip(&AdapterEvent::ClientQuotaAltered(
        AdminClientQuotaAlteration {
            operation_id: operation("quota-alter"),
            user: "testlab-user".to_owned(),
            direction: BrokerQuotaDirection::Producer,
        },
    ));
    round_trip(&AdapterEvent::ClientQuotaAlterationValidated(
        AdminClientQuotaAlteration {
            operation_id: operation("quota-validate"),
            user: "testlab-user".to_owned(),
            direction: BrokerQuotaDirection::Producer,
        },
    ));
    round_trip(&AdapterEvent::ClientQuotaDescribed(
        AdminClientQuotaDescription {
            operation_id: operation("quota-describe"),
            user: "testlab-user".to_owned(),
            direction: BrokerQuotaDirection::Producer,
            bytes_per_second: 65_536,
        },
    ));
    round_trip(&BrokerStateObservation::ClientQuota(
        BrokerClientQuotaState {
            observation: 4,
            operation_id: operation("quota-alter"),
            user: "testlab-user".to_owned(),
            direction: BrokerQuotaDirection::Producer,
            bytes_per_second: None,
        },
    ));
}

#[test]
fn quota_description_action_defaults_to_strict_filtering() {
    let encoded = toml::to_string(&ScenarioAction::DescribeClientQuota(describe()))
        .unwrap_or_else(|error| panic!("encode quota description: {error}"))
        .replace("strict = false\n", "");
    let decoded = toml::from_str::<ScenarioAction>(&encoded)
        .unwrap_or_else(|error| panic!("decode default quota strictness: {error}"));
    assert!(matches!(
        decoded,
        ScenarioAction::DescribeClientQuota(DescribeClientQuotaAction { strict: true, .. })
    ));
}

#[test]
fn validation_only_requires_and_preserves_a_prior_exact_rate() {
    let valid = quota_scenario(vec![
        alter(Some(65_536)),
        validated("quota-validate-one", 32_768, 65_536),
        validated("quota-validate-two", 16_384, 65_536),
    ]);
    let mut problems = Vec::new();
    crate::admin_client_quota_validation::validate_transitions(&valid, &mut problems);
    assert!(problems.is_empty(), "{problems:?}");

    let invalid = quota_scenario(vec![validated("quota-validate-missing", 32_768, 65_536)]);
    crate::admin_client_quota_validation::validate_transitions(&invalid, &mut problems);
    assert!(
        problems
            .iter()
            .any(|problem| problem.contains("requires a prior exact rate")),
        "{problems:?}"
    );
}

#[test]
fn validation_accepts_removal_and_rejects_unsafe_or_out_of_range_values() {
    let clients = BTreeMap::from([(client(), false)]);
    let mut operation_ids = BTreeSet::new();
    let mut problems = Vec::new();
    crate::admin_action_validation::validate(
        &ScenarioAction::AlterClientQuota(alter(None)),
        &clients,
        &mut operation_ids,
        &mut problems,
    );
    assert!(problems.is_empty(), "{problems:?}");

    for (operation_id, user, rate) in [
        ("quota-zero", "testlab-user", Some(0)),
        ("quota-large", "testlab-user", Some(u64::from(u32::MAX) + 1)),
        ("quota-unsafe", "bad user", Some(1)),
    ] {
        let mut action = alter(rate);
        action.operation_id = operation(operation_id);
        action.user = user.to_owned();
        crate::admin_action_validation::validate(
            &ScenarioAction::AlterClientQuota(action),
            &clients,
            &mut operation_ids,
            &mut problems,
        );
    }
    for expected in ["rate must be between 1", "invalid client-quota user"] {
        assert!(
            problems.iter().any(|problem| problem.contains(expected)),
            "missing {expected:?} in {problems:?}"
        );
    }
}

fn alter(rate: Option<u64>) -> AlterClientQuotaAction {
    AlterClientQuotaAction {
        client_id: client(),
        operation_id: operation("quota-alter"),
        user: "testlab-user".to_owned(),
        direction: BrokerQuotaDirection::Producer,
        bytes_per_second: rate,
        validate_only: false,
        expected_current_bytes_per_second: None,
        timeout_ms: 1_000,
    }
}

fn validated(operation_id: &str, proposed: u64, current: u64) -> AlterClientQuotaAction {
    AlterClientQuotaAction {
        operation_id: operation(operation_id),
        bytes_per_second: Some(proposed),
        validate_only: true,
        expected_current_bytes_per_second: Some(current),
        ..alter(Some(proposed))
    }
}

fn alter_command(
    operation_id: &str,
    rate: Option<u64>,
    validate_only: bool,
) -> AlterClientQuotaCommand {
    AlterClientQuotaCommand {
        client_id: client(),
        operation_id: operation(operation_id),
        user: "testlab-user".to_owned(),
        direction: BrokerQuotaDirection::Producer,
        bytes_per_second: rate,
        validate_only,
        timeout_ms: 1_000,
    }
}

fn quota_scenario(actions: Vec<AlterClientQuotaAction>) -> Scenario {
    Scenario {
        schema_version: SCENARIO_SCHEMA_VERSION,
        id: ScenarioId::new("client-quota-validation-test")
            .unwrap_or_else(|error| panic!("scenario: {error}")),
        title: "client quota validation".to_owned(),
        description: "client quota validation".to_owned(),
        timeout_ms: 5_000,
        requires: BTreeSet::from([Capability::Admin]),
        steps: actions
            .into_iter()
            .enumerate()
            .map(|(index, action)| ScenarioStep {
                id: StepId::new(format!("quota-{index}"))
                    .unwrap_or_else(|error| panic!("step: {error}")),
                action: ScenarioAction::AlterClientQuota(action),
            })
            .collect(),
        assertions: Vec::new(),
    }
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

fn describe_command() -> DescribeClientQuotaCommand {
    DescribeClientQuotaCommand {
        client_id: client(),
        operation_id: operation("quota-describe"),
        user: "testlab-user".to_owned(),
        direction: BrokerQuotaDirection::Producer,
        strict: false,
        timeout_ms: 1_000,
    }
}

fn round_trip<T>(value: &T)
where
    T: serde::Serialize + serde::de::DeserializeOwned + std::fmt::Debug + PartialEq,
{
    let encoded = serde_json::to_string(value)
        .unwrap_or_else(|error| panic!("encode client quota value: {error}"));
    let decoded = serde_json::from_str::<T>(&encoded)
        .unwrap_or_else(|error| panic!("decode client quota value: {error}"));
    assert_eq!(&decoded, value);
}

fn client() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client: {error}"))
}

fn operation(value: &str) -> OperationId {
    OperationId::new(value).unwrap_or_else(|error| panic!("operation: {error}"))
}
