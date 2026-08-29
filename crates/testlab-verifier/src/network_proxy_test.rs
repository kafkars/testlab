//! Network verifier tests reject missing effects, process failures, and false recovery.

use std::collections::BTreeSet;

use testlab_schema::{
    AdapterCommand, AdapterEvent, EnvironmentOperation, EnvironmentOperationId,
    EnvironmentOperationKind, EnvironmentOperationStatus, HistoryEntry, HistoryPayload,
    NetworkFault, NetworkFaultAction, NetworkFaultState, NetworkFaultWindowObservation,
    NetworkProxyControl, NetworkProxyObservation, OperationId, ProducerId, Scenario,
    ScenarioAction, ScenarioId, TerminalStatus,
};

use crate::index::HistoryIndex;
use crate::verify_fixture::{command, event, record, step};

#[test]
fn exact_blackhole_window_and_recovery_pass_all_network_contracts() {
    let index = HistoryIndex::build(&history());
    let mut violations = Vec::new();
    crate::network_proxy::verify(&scenario(), &index, &mut violations);
    assert!(violations.is_empty(), "{violations:?}");
}

#[test]
fn undeclared_control_and_wrong_observation_target_fail_effect_contract() {
    let mut entries = history();
    entries.insert(
        5,
        control(
            5,
            NetworkProxyControl::CutConnections(testlab_schema::NetworkConnectionCutAction {
                operation_id: environment_id("undeclared-cut"),
                broker_ordinal: 2,
                timeout_ms: 1_000,
            }),
        ),
    );
    resequence(&mut entries);
    let Some(HistoryPayload::NetworkProxyObservation {
        observation: NetworkProxyObservation::FaultWindow(window),
    }) = entries
        .iter_mut()
        .find(|entry| {
            matches!(
                entry.payload,
                HistoryPayload::NetworkProxyObservation { .. }
            )
        })
        .map(|entry| &mut entry.payload)
    else {
        panic!("fault observation fixture missing");
    };
    window.broker_ordinal = 2;
    let index = HistoryIndex::build(&entries);
    let mut violations = Vec::new();
    crate::network_proxy::verify(&scenario(), &index, &mut violations);
    assert!(has(&violations, "NET-001"), "{violations:?}");
}

#[test]
fn missing_proxy_terminal_fails_process_integrity() {
    let mut entries = history();
    entries.retain(|entry| {
        !matches!(
            &entry.payload,
            HistoryPayload::EnvironmentOperation { operation }
                if operation.kind == EnvironmentOperationKind::NetworkProxy
        )
    });
    let index = HistoryIndex::build(&entries);
    let mut violations = Vec::new();
    crate::network_proxy::verify(&scenario(), &index, &mut violations);
    assert!(has(&violations, "NET-002"), "{violations:?}");
}

#[test]
fn stronger_blackhole_outcome_cannot_replace_declared_uncertainty() {
    let mut entries = history();
    set_terminal(
        &mut entries,
        "during-blackhole",
        TerminalStatus::Acknowledged,
    );
    let index = HistoryIndex::build(&entries);
    let mut violations = Vec::new();
    crate::network_proxy::verify(&scenario(), &index, &mut violations);
    assert!(has(&violations, "NET-003"), "{violations:?}");
}

#[test]
fn nonacknowledged_post_fault_send_fails_recovery() {
    let mut entries = history();
    set_terminal(
        &mut entries,
        "after-blackhole",
        TerminalStatus::PossiblySent,
    );
    let index = HistoryIndex::build(&entries);
    let mut violations = Vec::new();
    crate::network_proxy::verify(&scenario(), &index, &mut violations);
    assert!(has(&violations, "NET-004"), "{violations:?}");
}

fn scenario() -> Scenario {
    Scenario {
        schema_version: testlab_schema::SCENARIO_SCHEMA_VERSION,
        id: ScenarioId::new("network.blackhole")
            .unwrap_or_else(|error| panic!("scenario id: {error}")),
        title: "network blackhole".to_owned(),
        description: "network verifier fixture".to_owned(),
        timeout_ms: 10_000,
        requires: BTreeSet::new(),
        steps: vec![
            step(
                "apply",
                ScenarioAction::AlterNetworkFault(fault(
                    "blackhole-apply",
                    NetworkFaultState::Present,
                )),
            ),
            step("during", send("during-blackhole")),
            step(
                "remove",
                ScenarioAction::AlterNetworkFault(fault(
                    "blackhole-remove",
                    NetworkFaultState::Absent,
                )),
            ),
            step("after", send("after-blackhole")),
        ],
        assertions: Vec::new(),
    }
}

fn history() -> Vec<HistoryEntry> {
    vec![
        control(
            0,
            NetworkProxyControl::AlterFault(fault("blackhole-apply", NetworkFaultState::Present)),
        ),
        command(
            1,
            AdapterCommand::Send {
                producer_id: producer(),
                operation_id: operation("during-blackhole"),
                record: record("during"),
            },
        ),
        terminal(2, "during-blackhole", TerminalStatus::PossiblySent),
        control(
            3,
            NetworkProxyControl::AlterFault(fault("blackhole-remove", NetworkFaultState::Absent)),
        ),
        observation(4),
        command(
            5,
            AdapterCommand::Send {
                producer_id: producer(),
                operation_id: operation("after-blackhole"),
                record: record("after"),
            },
        ),
        terminal(6, "after-blackhole", TerminalStatus::Acknowledged),
        proxy_process(7),
    ]
}

fn fault(id: &str, state: NetworkFaultState) -> NetworkFaultAction {
    NetworkFaultAction {
        operation_id: environment_id(id),
        broker_ordinal: 1,
        fault: NetworkFault::Blackhole,
        state,
        timeout_ms: 1_000,
    }
}

fn send(id: &str) -> ScenarioAction {
    ScenarioAction::Send {
        producer_id: producer(),
        operation_id: operation(id),
        record: record(id),
    }
}

fn control(sequence: u64, control: NetworkProxyControl) -> HistoryEntry {
    HistoryEntry {
        sequence,
        observed_unix_ms: sequence,
        payload: HistoryPayload::NetworkProxyControl { control },
    }
}

fn observation(sequence: u64) -> HistoryEntry {
    HistoryEntry {
        sequence,
        observed_unix_ms: sequence,
        payload: HistoryPayload::NetworkProxyObservation {
            observation: NetworkProxyObservation::FaultWindow(NetworkFaultWindowObservation {
                observation: 0,
                apply_operation_id: environment_id("blackhole-apply"),
                remove_operation_id: environment_id("blackhole-remove"),
                broker_ordinal: 1,
                fault: NetworkFault::Blackhole,
                started_unix_ms: 1,
                completed_unix_ms: 3,
                connections_at_start: 1,
                connections_accepted: 0,
                client_to_broker_bytes: 0,
                broker_to_client_bytes: 0,
                delayed_client_to_broker_bytes: 0,
                delayed_broker_to_client_bytes: 0,
                blocked_intervals: 1,
            }),
        },
    }
}

fn terminal(sequence: u64, id: &str, status: TerminalStatus) -> HistoryEntry {
    event(
        sequence,
        AdapterEvent::OperationTerminal {
            operation_id: operation(id),
            status,
            code: None,
            offset: None,
        },
    )
}

fn proxy_process(sequence: u64) -> HistoryEntry {
    HistoryEntry {
        sequence,
        observed_unix_ms: sequence,
        payload: HistoryPayload::EnvironmentOperation {
            operation: EnvironmentOperation {
                id: environment_id("network-proxy-process"),
                kind: EnvironmentOperationKind::NetworkProxy,
                program: "/tmp/testctl".to_owned(),
                args: vec![
                    "network-proxy-worker".to_owned(),
                    "--route".to_owned(),
                    "1|127.0.0.1:29092|127.0.0.1:39092".to_owned(),
                ],
                started_unix_ms: 0,
                completed_unix_ms: sequence,
                status: EnvironmentOperationStatus::Succeeded,
                exit_code: Some(0),
                stdout_artifact: Some("network-proxy.jsonl".to_owned()),
                stderr_artifact: Some("network-proxy.stderr.txt".to_owned()),
                diagnostic: None,
            },
        },
    }
}

fn set_terminal(entries: &mut [HistoryEntry], id: &str, status: TerminalStatus) {
    for entry in entries {
        let HistoryPayload::AdapterEvent { event } = &mut entry.payload else {
            continue;
        };
        if let AdapterEvent::OperationTerminal {
            operation_id,
            status: actual,
            ..
        } = &mut event.event
            && operation_id.as_str() == id
        {
            *actual = status;
        }
    }
}

fn resequence(entries: &mut [HistoryEntry]) {
    for (sequence, entry) in entries.iter_mut().enumerate() {
        entry.sequence = u64::try_from(sequence).unwrap_or(u64::MAX);
    }
}

fn operation(value: &str) -> OperationId {
    OperationId::new(value).unwrap_or_else(|error| panic!("operation id: {error}"))
}

fn environment_id(value: &str) -> EnvironmentOperationId {
    EnvironmentOperationId::new(value)
        .unwrap_or_else(|error| panic!("environment operation id: {error}"))
}

fn producer() -> ProducerId {
    ProducerId::new("producer-1").unwrap_or_else(|error| panic!("producer id: {error}"))
}

fn has(violations: &[testlab_schema::Violation], contract: &str) -> bool {
    violations
        .iter()
        .any(|violation| violation.contract_id.as_str() == contract)
}
