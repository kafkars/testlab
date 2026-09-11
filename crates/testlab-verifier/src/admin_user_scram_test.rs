//! User SCRAM verifier tests separate public terminals from independent CLI state.

use std::collections::BTreeSet;

use testlab_schema::{
    AdapterCommand, AdapterEvent, AdminUserScramCredentialAlteration,
    AdminUserScramCredentialDescription, AlterUserScramCredentialAction, BrokerStateObservation,
    BrokerUserScramCredentialState, Capability, ClientId, DescribeUserScramCredentialAction,
    DescribeUserScramCredentialCommand, HistoryEntry, HistoryPayload, OperationId,
    SCENARIO_SCHEMA_VERSION, Scenario, ScenarioAction, ScenarioId, ScramCredentialMechanism,
};

use crate::admin::verify_admin;
use crate::index::HistoryIndex;
use crate::verify_fixture::{command, event, step};

#[test]
fn exact_public_scram_lifecycle_passes_with_immediate_independent_state() {
    assert!(violations(&history()).is_empty());
}

#[test]
fn wrong_public_absence_code_fails_description_contract() {
    let mut entries = history();
    let HistoryPayload::AdapterEvent { event } = &mut entries[10].payload else {
        panic!("user SCRAM absence history kind");
    };
    let AdapterEvent::UserScramCredentialDescribed(value) = &mut event.event else {
        panic!("user SCRAM description event kind");
    };
    value.absence_broker_code = None;

    assert_contract(&violations(&entries), "ADMIN-035");
}

#[test]
fn wrong_public_identity_or_post_delete_state_fails_alteration_contract() {
    let mut entries = history();
    let HistoryPayload::AdapterEvent { event } = &mut entries[1].payload else {
        panic!("user SCRAM alteration history kind");
    };
    let AdapterEvent::UserScramCredentialAltered(value) = &mut event.event else {
        panic!("user SCRAM alteration event kind");
    };
    value.user = "other-user".to_owned();
    let HistoryPayload::BrokerStateObservation { observation } = &mut entries[8].payload else {
        panic!("user SCRAM deletion observation history kind");
    };
    let BrokerStateObservation::UserScramCredential(value) = observation else {
        panic!("user SCRAM observation kind");
    };
    value.iterations = Some(8_192);

    assert_contract(&violations(&entries), "ADMIN-036");
}

fn history() -> Vec<HistoryEntry> {
    vec![
        command(
            1,
            AdapterCommand::AlterUserScramCredential(alter("scram-upsert", Some(8_192))),
        ),
        event(2, altered("scram-upsert")),
        state(3, 0, "scram-upsert", Some(8_192)),
        command(
            4,
            AdapterCommand::DescribeUserScramCredential(describe_command("scram-present")),
        ),
        event(5, described("scram-present", Some(8_192), None)),
        state(6, 1, "scram-present", Some(8_192)),
        command(
            7,
            AdapterCommand::AlterUserScramCredential(alter("scram-delete", None)),
        ),
        event(8, altered("scram-delete")),
        state(9, 2, "scram-delete", None),
        command(
            10,
            AdapterCommand::DescribeUserScramCredential(describe_command("scram-absent")),
        ),
        event(11, described("scram-absent", None, Some(91))),
        state(12, 3, "scram-absent", None),
    ]
}

fn altered(operation_id: &str) -> AdapterEvent {
    AdapterEvent::UserScramCredentialAltered(AdminUserScramCredentialAlteration {
        operation_id: operation(operation_id),
        user: "testlab-user".to_owned(),
        mechanism: mechanism(),
    })
}

fn described(
    operation_id: &str,
    iterations: Option<u32>,
    absence_broker_code: Option<i16>,
) -> AdapterEvent {
    AdapterEvent::UserScramCredentialDescribed(AdminUserScramCredentialDescription {
        operation_id: operation(operation_id),
        user: "testlab-user".to_owned(),
        mechanism: mechanism(),
        iterations,
        absence_broker_code,
    })
}

fn state(
    sequence: u64,
    observation: u64,
    operation_id: &str,
    iterations: Option<u32>,
) -> HistoryEntry {
    HistoryEntry {
        sequence,
        observed_unix_ms: sequence,
        payload: HistoryPayload::BrokerStateObservation {
            observation: BrokerStateObservation::UserScramCredential(
                BrokerUserScramCredentialState {
                    observation,
                    operation_id: operation(operation_id),
                    user: "testlab-user".to_owned(),
                    mechanism: mechanism(),
                    iterations,
                },
            ),
        },
    }
}

fn scenario() -> Scenario {
    Scenario {
        schema_version: SCENARIO_SCHEMA_VERSION,
        id: ScenarioId::new("kafka.admin-user-scram-credential-lifecycle")
            .unwrap_or_else(|error| panic!("scenario: {error}")),
        title: "user SCRAM credential lifecycle".to_owned(),
        description: "public SCRAM administration with independent broker state".to_owned(),
        timeout_ms: 10_000,
        requires: BTreeSet::from([Capability::Admin, Capability::Lifecycle]),
        steps: vec![
            step(
                "upsert",
                ScenarioAction::AlterUserScramCredential(alter("scram-upsert", Some(8_192))),
            ),
            step(
                "describe-present",
                ScenarioAction::DescribeUserScramCredential(describe("scram-present", Some(8_192))),
            ),
            step(
                "delete",
                ScenarioAction::AlterUserScramCredential(alter("scram-delete", None)),
            ),
            step(
                "describe-absent",
                ScenarioAction::DescribeUserScramCredential(describe("scram-absent", None)),
            ),
        ],
        assertions: Vec::new(),
    }
}

fn alter(operation_id: &str, iterations: Option<u32>) -> AlterUserScramCredentialAction {
    AlterUserScramCredentialAction {
        client_id: client(),
        operation_id: operation(operation_id),
        user: "testlab-user".to_owned(),
        mechanism: mechanism(),
        iterations,
        timeout_ms: 1_000,
    }
}

fn describe(
    operation_id: &str,
    expected_iterations: Option<u32>,
) -> DescribeUserScramCredentialAction {
    DescribeUserScramCredentialAction {
        client_id: client(),
        operation_id: operation(operation_id),
        user: "testlab-user".to_owned(),
        mechanism: mechanism(),
        expected_iterations,
        timeout_ms: 1_000,
    }
}

fn describe_command(operation_id: &str) -> DescribeUserScramCredentialCommand {
    DescribeUserScramCredentialCommand {
        client_id: client(),
        operation_id: operation(operation_id),
        user: "testlab-user".to_owned(),
        mechanism: mechanism(),
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

const fn mechanism() -> ScramCredentialMechanism {
    ScramCredentialMechanism::Sha256
}

fn client() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client: {error}"))
}

fn operation(value: &str) -> OperationId {
    OperationId::new(value).unwrap_or_else(|error| panic!("operation: {error}"))
}
