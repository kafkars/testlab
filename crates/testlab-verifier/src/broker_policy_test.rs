//! Broker-policy verifier tests pin raw control trust and bounded quota progress.

use std::collections::BTreeSet;

use testlab_schema::{
    AdapterCommand, AdapterEvent, BrokerPolicy, BrokerPolicyAction, BrokerPolicyState,
    BrokerQuotaDirection, EnvironmentOperation, EnvironmentOperationId, EnvironmentOperationKind,
    EnvironmentOperationStatus, HistoryEntry, HistoryPayload, ProducerId, Scenario, ScenarioAction,
    ScenarioId, TerminalStatus,
};

use crate::index::HistoryIndex;
use crate::verify_fixture::{command, event, record, step};

#[test]
fn exact_quota_control_and_public_progress_pass() {
    let policy = quota();
    let index = HistoryIndex::build(&history(&policy, 1_000));
    let mut violations = Vec::new();

    crate::broker_policy::verify(&scenario(policy), &index, &[], &mut violations);

    assert!(violations.is_empty(), "{violations:?}");
}

#[test]
fn a_tampered_query_cannot_support_normalized_policy_truth() {
    let policy = quota();
    let mut entries = history(&policy, 1_000);
    let Some(HistoryPayload::EnvironmentOperation { operation }) =
        entries.get_mut(1).map(|entry| &mut entry.payload)
    else {
        panic!("query fixture missing");
    };
    operation.args = strings(&["kafka-configs.sh", "--describe", "--entity-name", "other"]);
    let index = HistoryIndex::build(&entries);
    let mut violations = Vec::new();

    crate::broker_policy::verify(&scenario(policy), &index, &[], &mut violations);

    assert!(has(&violations, "POLICY-001"), "{violations:?}");
}

#[test]
fn quota_progress_shorter_than_the_declared_window_fails() {
    let policy = quota();
    let index = HistoryIndex::build(&history(&policy, 100));
    let mut violations = Vec::new();

    crate::broker_policy::verify(&scenario(policy), &index, &[], &mut violations);

    assert!(has(&violations, "POLICY-004"), "{violations:?}");
}

fn scenario(policy: BrokerPolicy) -> Scenario {
    Scenario {
        schema_version: testlab_schema::SCENARIO_SCHEMA_VERSION,
        id: ScenarioId::new("policy.quota").unwrap_or_else(|error| panic!("scenario id: {error}")),
        title: "quota".to_owned(),
        description: "quota fixture".to_owned(),
        timeout_ms: 2_000,
        requires: BTreeSet::new(),
        steps: vec![
            step(
                "apply",
                ScenarioAction::AlterBrokerPolicy(BrokerPolicyAction {
                    policy: policy.clone(),
                    state: BrokerPolicyState::Present,
                    timeout_ms: 1_000,
                }),
            ),
            step(
                "send",
                ScenarioAction::Send {
                    producer_id: producer(),
                    operation_id: operation(),
                    record: record("quota"),
                },
            ),
            step(
                "remove",
                ScenarioAction::AlterBrokerPolicy(BrokerPolicyAction {
                    policy,
                    state: BrokerPolicyState::Absent,
                    timeout_ms: 1_000,
                }),
            ),
        ],
        assertions: Vec::new(),
    }
}

fn history(policy: &BrokerPolicy, removal_started_ms: u64) -> Vec<HistoryEntry> {
    vec![
        environment(
            0,
            "apply",
            EnvironmentOperationKind::BrokerPolicyAlter,
            alter_args(true),
            0,
            1,
            "docker",
        ),
        environment(
            1,
            "apply-query",
            EnvironmentOperationKind::BrokerPolicyQuery,
            query_args(),
            1,
            2,
            "docker",
        ),
        environment(
            2,
            "apply-observe",
            EnvironmentOperationKind::BrokerPolicyObserve,
            policy.evidence_args(BrokerPolicyState::Present),
            2,
            2,
            "testlab-kafka-policy-observer/1",
        ),
        command(
            3,
            AdapterCommand::Send {
                producer_id: producer(),
                operation_id: operation(),
                record: record("quota"),
            },
        ),
        event(
            4,
            AdapterEvent::OperationTerminal {
                operation_id: operation(),
                status: TerminalStatus::Acknowledged,
                code: None,
                offset: Some(0),
            },
        ),
        environment(
            5,
            "remove",
            EnvironmentOperationKind::BrokerPolicyAlter,
            alter_args(false),
            removal_started_ms,
            removal_started_ms + 1,
            "docker",
        ),
        environment(
            6,
            "remove-query",
            EnvironmentOperationKind::BrokerPolicyQuery,
            query_args(),
            removal_started_ms + 1,
            removal_started_ms + 2,
            "docker",
        ),
        environment(
            7,
            "remove-observe",
            EnvironmentOperationKind::BrokerPolicyObserve,
            policy.evidence_args(BrokerPolicyState::Absent),
            removal_started_ms + 2,
            removal_started_ms + 2,
            "testlab-kafka-policy-observer/1",
        ),
    ]
}

#[allow(clippy::too_many_arguments, reason = "explicit evidence fixture")]
fn environment(
    sequence: u64,
    id: &str,
    kind: EnvironmentOperationKind,
    args: Vec<String>,
    started: u64,
    completed: u64,
    program: &str,
) -> HistoryEntry {
    HistoryEntry {
        sequence,
        observed_unix_ms: completed,
        payload: HistoryPayload::EnvironmentOperation {
            operation: EnvironmentOperation {
                id: EnvironmentOperationId::new(id)
                    .unwrap_or_else(|error| panic!("environment id: {error}")),
                kind,
                program: program.to_owned(),
                args,
                started_unix_ms: started,
                completed_unix_ms: completed,
                status: EnvironmentOperationStatus::Succeeded,
                exit_code: Some(0),
                stdout_artifact: None,
                stderr_artifact: None,
                diagnostic: None,
            },
        },
    }
}

fn alter_args(present: bool) -> Vec<String> {
    strings(&[
        "exec",
        "--no-TTY",
        "broker",
        "/opt/kafka/bin/kafka-configs.sh",
        "--bootstrap-server",
        "localhost:19092",
        "--alter",
        "--entity-type",
        "users",
        "--entity-name",
        "kafkars",
        if present {
            "--add-config"
        } else {
            "--delete-config"
        },
        if present {
            "producer_byte_rate=128"
        } else {
            "producer_byte_rate"
        },
    ])
}

fn query_args() -> Vec<String> {
    strings(&[
        "exec",
        "--no-TTY",
        "broker",
        "/opt/kafka/bin/kafka-configs.sh",
        "--bootstrap-server",
        "localhost:19092",
        "--describe",
        "--entity-type",
        "users",
        "--entity-name",
        "kafkars",
    ])
}

fn quota() -> BrokerPolicy {
    BrokerPolicy::Quota {
        direction: BrokerQuotaDirection::Producer,
        bytes_per_second: 128,
        minimum_active_ms: 500,
    }
}

fn producer() -> ProducerId {
    ProducerId::new("producer-1").unwrap_or_else(|error| panic!("producer id: {error}"))
}

fn operation() -> testlab_schema::OperationId {
    testlab_schema::OperationId::new("op-1").unwrap_or_else(|error| panic!("operation id: {error}"))
}

fn strings(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| (*value).to_owned()).collect()
}

fn has(violations: &[testlab_schema::Violation], contract: &str) -> bool {
    violations
        .iter()
        .any(|violation| violation.contract_id.as_str() == contract)
}
