//! Client-quota verifier tests separate public terminals from independent CLI state.

use std::collections::BTreeSet;

use testlab_schema::{
    AdapterCommand, AdapterEvent, AdminClientQuotaAlteration, AdminClientQuotaDescription,
    AlterClientQuotaAction, AlterClientQuotaCommand, BrokerClientQuotaState, BrokerQuotaDirection,
    BrokerStateObservation, Capability, ClientId, DescribeClientQuotaAction,
    DescribeClientQuotaCommand, HistoryEntry, HistoryPayload, OperationId, SCENARIO_SCHEMA_VERSION,
    Scenario, ScenarioAction, ScenarioId,
};

use crate::admin::verify_admin;
use crate::index::HistoryIndex;
use crate::verify_fixture::{command, event, step};

#[test]
fn exact_public_quota_lifecycle_passes_with_immediate_independent_state() {
    assert!(violations(&history()).is_empty());
}

#[test]
fn wrong_public_description_fails_description_contract() {
    let mut entries = history();
    let HistoryPayload::AdapterEvent { event } = &mut entries[4].payload else {
        panic!("client-quota description history kind");
    };
    let AdapterEvent::ClientQuotaDescribed(value) = &mut event.event else {
        panic!("client-quota description event kind");
    };
    value.bytes_per_second = 1;

    assert_contract(&violations(&entries), "ADMIN-090");
}

#[test]
fn wrong_public_identity_or_post_remove_state_fails_alteration_contract() {
    let mut entries = history();
    let HistoryPayload::AdapterEvent { event } = &mut entries[1].payload else {
        panic!("client-quota alteration history kind");
    };
    let AdapterEvent::ClientQuotaAltered(value) = &mut event.event else {
        panic!("client-quota alteration event kind");
    };
    value.user = "other-user".to_owned();
    let HistoryPayload::BrokerStateObservation { observation } = &mut entries[11].payload else {
        panic!("client-quota removal observation history kind");
    };
    let BrokerStateObservation::ClientQuota(value) = observation else {
        panic!("client-quota observation kind");
    };
    value.bytes_per_second = Some(65_536);

    let violations = violations(&entries);
    assert_contract(&violations, "ADMIN-034");
}

#[test]
fn mutated_state_or_wrong_terminal_fails_validation_only_contract() {
    let mut entries = history();
    let HistoryPayload::BrokerStateObservation { observation } = &mut entries[8].payload else {
        panic!("client-quota validation observation history kind");
    };
    let BrokerStateObservation::ClientQuota(value) = observation else {
        panic!("client-quota observation kind");
    };
    value.bytes_per_second = Some(32_768);
    assert_contract(&violations(&entries), "ADMIN-086");

    let mut entries = history();
    let HistoryPayload::AdapterEvent { event } = &mut entries[7].payload else {
        panic!("client-quota validation event history kind");
    };
    event.event = AdapterEvent::ClientQuotaAltered(AdminClientQuotaAlteration {
        operation_id: operation("quota-validate"),
        user: "testlab-user".to_owned(),
        direction: BrokerQuotaDirection::Producer,
    });
    assert_contract(&violations(&entries), "ADMIN-086");
}

fn history() -> Vec<HistoryEntry> {
    vec![
        command(
            1,
            AdapterCommand::AlterClientQuota(alter_command("quota-set", Some(65_536), false)),
        ),
        event(
            2,
            AdapterEvent::ClientQuotaAltered(AdminClientQuotaAlteration {
                operation_id: operation("quota-set"),
                user: "testlab-user".to_owned(),
                direction: BrokerQuotaDirection::Producer,
            }),
        ),
        state(3, 0, "quota-set", Some(65_536)),
        command(
            4,
            AdapterCommand::DescribeClientQuota(DescribeClientQuotaCommand {
                client_id: client(),
                operation_id: operation("quota-describe"),
                user: "testlab-user".to_owned(),
                direction: BrokerQuotaDirection::Producer,
                strict: false,
                timeout_ms: 1_000,
            }),
        ),
        event(
            5,
            AdapterEvent::ClientQuotaDescribed(AdminClientQuotaDescription {
                operation_id: operation("quota-describe"),
                user: "testlab-user".to_owned(),
                direction: BrokerQuotaDirection::Producer,
                bytes_per_second: 65_536,
            }),
        ),
        state(6, 1, "quota-describe", Some(65_536)),
        command(
            7,
            AdapterCommand::AlterClientQuota(alter_command("quota-validate", Some(32_768), true)),
        ),
        event(
            8,
            AdapterEvent::ClientQuotaAlterationValidated(AdminClientQuotaAlteration {
                operation_id: operation("quota-validate"),
                user: "testlab-user".to_owned(),
                direction: BrokerQuotaDirection::Producer,
            }),
        ),
        state(9, 2, "quota-validate", Some(65_536)),
        command(
            10,
            AdapterCommand::AlterClientQuota(alter_command("quota-remove", None, false)),
        ),
        event(
            11,
            AdapterEvent::ClientQuotaAltered(AdminClientQuotaAlteration {
                operation_id: operation("quota-remove"),
                user: "testlab-user".to_owned(),
                direction: BrokerQuotaDirection::Producer,
            }),
        ),
        state(12, 3, "quota-remove", None),
    ]
}

fn state(sequence: u64, observation: u64, operation_id: &str, rate: Option<u64>) -> HistoryEntry {
    HistoryEntry {
        sequence,
        observed_unix_ms: sequence,
        payload: HistoryPayload::BrokerStateObservation {
            observation: BrokerStateObservation::ClientQuota(BrokerClientQuotaState {
                observation,
                operation_id: operation(operation_id),
                user: "testlab-user".to_owned(),
                direction: BrokerQuotaDirection::Producer,
                bytes_per_second: rate,
            }),
        },
    }
}

fn scenario() -> Scenario {
    Scenario {
        schema_version: SCENARIO_SCHEMA_VERSION,
        id: ScenarioId::new("kafka.admin-client-quota-lifecycle")
            .unwrap_or_else(|error| panic!("scenario: {error}")),
        title: "client-quota lifecycle".to_owned(),
        description: "public quota administration with independent broker state".to_owned(),
        timeout_ms: 10_000,
        requires: BTreeSet::from([Capability::Admin, Capability::Lifecycle]),
        steps: vec![
            step(
                "set-quota",
                ScenarioAction::AlterClientQuota(alter("quota-set", Some(65_536))),
            ),
            step(
                "describe-quota",
                ScenarioAction::DescribeClientQuota(describe()),
            ),
            step(
                "validate-quota",
                ScenarioAction::AlterClientQuota(validated()),
            ),
            step(
                "remove-quota",
                ScenarioAction::AlterClientQuota(alter("quota-remove", None)),
            ),
        ],
        assertions: Vec::new(),
    }
}

fn alter(operation_id: &str, rate: Option<u64>) -> AlterClientQuotaAction {
    AlterClientQuotaAction {
        client_id: client(),
        operation_id: operation(operation_id),
        user: "testlab-user".to_owned(),
        direction: BrokerQuotaDirection::Producer,
        bytes_per_second: rate,
        validate_only: false,
        expected_current_bytes_per_second: None,
        timeout_ms: 1_000,
    }
}

fn validated() -> AlterClientQuotaAction {
    AlterClientQuotaAction {
        operation_id: operation("quota-validate"),
        bytes_per_second: Some(32_768),
        validate_only: true,
        expected_current_bytes_per_second: Some(65_536),
        ..alter("quota-validate", Some(32_768))
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

fn violations(entries: &[HistoryEntry]) -> Vec<testlab_schema::Violation> {
    let index = HistoryIndex::build(entries);
    let mut violations = Vec::new();
    verify_admin(&scenario(), &index, &[], &mut violations);
    violations
}

fn assert_contract(violations: &[testlab_schema::Violation], contract: &str) {
    assert!(
        violations
            .iter()
            .any(|violation| violation.contract_id.as_str() == contract),
        "{violations:?}"
    );
}

fn client() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client: {error}"))
}

fn operation(value: &str) -> OperationId {
    OperationId::new(value).unwrap_or_else(|error| panic!("operation: {error}"))
}
