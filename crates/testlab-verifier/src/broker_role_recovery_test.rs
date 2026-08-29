//! Broker-role verifier tests pin ownership facts, terminal pairing, and progress windows.

use std::collections::BTreeSet;

use testlab_schema::{
    AdapterEvent, AdminTopicCompletion, BrokerRoleTarget, ConsumedRecord, EnvironmentOperation,
    EnvironmentOperationId, EnvironmentOperationKind, EnvironmentOperationStatus, HistoryEntry,
    HistoryPayload, OperationId, Scenario, ScenarioAction, ScenarioId, TransactionDisposition,
};

use crate::broker_role_recovery::verify;
use crate::index::HistoryIndex;
use crate::verify_fixture::{event, step};

#[test]
fn exact_role_replacement_and_offline_progress_pass() {
    let index = HistoryIndex::build(&history(2, true));
    let mut violations = Vec::new();

    verify(&scenario(target()), &index, &mut violations);

    assert!(violations.is_empty(), "{violations:?}");
}

#[test]
fn unchanged_role_owner_fails_independent_observation() {
    let index = HistoryIndex::build(&history(1, true));
    let mut violations = Vec::new();

    verify(&scenario(target()), &index, &mut violations);

    assert!(has(&violations, "FAULT-001"), "{violations:?}");
}

#[test]
fn missing_offline_progress_fails_recovery_contract() {
    let index = HistoryIndex::build(&history(2, false));
    let mut violations = Vec::new();

    verify(&scenario(target()), &index, &mut violations);

    assert!(has(&violations, "FAULT-003"), "{violations:?}");
}

#[test]
fn malformed_readiness_cannot_satisfy_disruption_terminals() {
    let mut entries = history(2, true);
    let Some(HistoryPayload::EnvironmentOperation { operation }) =
        entries.last_mut().map(|entry| &mut entry.payload)
    else {
        panic!("readiness fixture missing");
    };
    operation.args = strings(&["readiness", "broker-1"]);
    let index = HistoryIndex::build(&entries);
    let mut violations = Vec::new();

    verify(&scenario(target()), &index, &mut violations);

    assert!(has(&violations, "FAULT-002"), "{violations:?}");
}

#[test]
fn every_role_specific_public_progress_shape_passes() {
    for (target, progress) in role_progress() {
        let index = HistoryIndex::build(&role_history(&target, 2, Some(progress)));
        let mut violations = Vec::new();

        verify(&scenario(target.clone()), &index, &mut violations);

        assert!(violations.is_empty(), "{target:?}: {violations:?}");
    }
}

fn scenario(target: BrokerRoleTarget) -> Scenario {
    Scenario {
        schema_version: testlab_schema::SCENARIO_SCHEMA_VERSION,
        id: ScenarioId::new("fault.role-recovery")
            .unwrap_or_else(|error| panic!("scenario id: {error}")),
        title: "role recovery".to_owned(),
        description: "role recovery fixture".to_owned(),
        timeout_ms: 10_000,
        requires: BTreeSet::new(),
        steps: vec![
            step(
                "stop-role",
                ScenarioAction::StopBrokerRole {
                    target: target.clone(),
                    timeout_ms: 1_000,
                },
            ),
            step(
                "restore-role",
                ScenarioAction::RestoreBrokerRole {
                    target,
                    timeout_ms: 1_000,
                },
            ),
        ],
        assertions: Vec::new(),
    }
}

fn history(replacement: i32, progress: bool) -> Vec<HistoryEntry> {
    let progress = progress.then(|| AdapterEvent::OperationTerminal {
        operation_id: operation_id("op-after-election"),
        status: testlab_schema::TerminalStatus::Acknowledged,
        code: None,
        offset: Some(1),
    });
    role_history(&target(), replacement, progress)
}

fn role_history(
    target: &BrokerRoleTarget,
    replacement: i32,
    progress: Option<AdapterEvent>,
) -> Vec<HistoryEntry> {
    let replacement_service = format!("broker-{replacement}");
    let mut entries = vec![
        environment(
            0,
            "observe-before",
            EnvironmentOperationKind::BrokerRoleObserve,
            role_args(target, "before_stop", 1, "broker-1"),
        ),
        environment(
            1,
            "stop-owner",
            EnvironmentOperationKind::BrokerStop,
            strings(&["compose", "stop", "broker-1"]),
        ),
        environment(
            2,
            "observe-after",
            EnvironmentOperationKind::BrokerRoleObserve,
            role_args(target, "after_election", replacement, &replacement_service),
        ),
    ];
    if let Some(progress) = progress {
        entries.push(event(3, progress));
    }
    entries.extend([
        environment(
            4,
            "start-owner",
            EnvironmentOperationKind::BrokerStart,
            strings(&["compose", "start", "broker-1"]),
        ),
        environment(
            5,
            "owner-ready",
            EnvironmentOperationKind::Readiness,
            strings(&[
                "compose",
                "exec",
                "--no-TTY",
                "broker-1",
                "/opt/kafka/bin/kafka-broker-api-versions.sh",
                "--bootstrap-server",
                "localhost:19092",
            ]),
        ),
    ]);
    entries
}

fn environment(
    sequence: u64,
    id: &str,
    kind: EnvironmentOperationKind,
    args: Vec<String>,
) -> HistoryEntry {
    environment_owned(sequence, id, kind, args)
}

fn environment_owned(
    sequence: u64,
    id: &str,
    kind: EnvironmentOperationKind,
    args: Vec<String>,
) -> HistoryEntry {
    HistoryEntry {
        sequence,
        observed_unix_ms: sequence,
        payload: HistoryPayload::EnvironmentOperation {
            operation: EnvironmentOperation {
                id: EnvironmentOperationId::new(id)
                    .unwrap_or_else(|error| panic!("environment id: {error}")),
                kind,
                program: if kind == EnvironmentOperationKind::BrokerRoleObserve {
                    "testlab-kafka-role-observer/1"
                } else {
                    "docker"
                }
                .to_owned(),
                args,
                started_unix_ms: sequence,
                completed_unix_ms: sequence,
                status: EnvironmentOperationStatus::Succeeded,
                exit_code: None,
                stdout_artifact: None,
                stderr_artifact: None,
                diagnostic: None,
            },
        },
    }
}

fn target() -> BrokerRoleTarget {
    BrokerRoleTarget::PartitionLeader {
        topic: "records".to_owned(),
        partition: 0,
    }
}

fn role_args(target: &BrokerRoleTarget, stage: &str, node: i32, service: &str) -> Vec<String> {
    let mut args = vec![target.role_name().to_owned()];
    args.extend(target.evidence_target());
    args.extend([stage.to_owned(), node.to_string(), service.to_owned()]);
    args
}

fn role_progress() -> Vec<(BrokerRoleTarget, AdapterEvent)> {
    vec![
        (
            BrokerRoleTarget::Controller,
            AdapterEvent::TopicCreated(AdminTopicCompletion {
                operation_id: operation_id("create-after-election"),
                topic: "controller-progress".to_owned(),
            }),
        ),
        (
            BrokerRoleTarget::GroupCoordinator {
                group_id: "group-1".to_owned(),
            },
            AdapterEvent::GroupReceiveCompleted {
                receive_id: operation_id("receive-after-election"),
                records: vec![ConsumedRecord {
                    topic: "records".to_owned(),
                    partition: 0,
                    offset: 1,
                    timestamp_millis: None,
                    key: None,
                    value: None,
                    headers: Vec::new(),
                }],
                committed: true,
                group_epoch: None,
            },
        ),
        (
            BrokerRoleTarget::TransactionCoordinator {
                transactional_id: "transactional-1".to_owned(),
            },
            AdapterEvent::TransactionCompleted {
                transaction_id: operation_id("transaction-after-election"),
                disposition: TransactionDisposition::Commit,
            },
        ),
    ]
}

fn operation_id(value: &str) -> OperationId {
    OperationId::new(value).unwrap_or_else(|error| panic!("operation id: {error}"))
}

fn strings(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| (*value).to_owned()).collect()
}

fn has(violations: &[testlab_schema::Violation], contract: &str) -> bool {
    violations
        .iter()
        .any(|violation| violation.contract_id.as_str() == contract)
}
