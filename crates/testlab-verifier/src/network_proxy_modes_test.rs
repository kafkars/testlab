//! Network mode tests cover delay progress and live-connection cut recovery.

use std::collections::BTreeSet;

use testlab_schema::{
    AdapterCommand, AdapterEvent, EnvironmentOperation, EnvironmentOperationId,
    EnvironmentOperationKind, EnvironmentOperationStatus, HistoryEntry, HistoryPayload,
    NetworkConnectionCutAction, NetworkConnectionsCutObservation, NetworkDirection, NetworkFault,
    NetworkFaultAction, NetworkFaultState, NetworkFaultWindowObservation, NetworkProxyControl,
    NetworkProxyObservation, OperationId, ProducerId, Scenario, ScenarioAction, ScenarioId,
    TerminalStatus,
};

use crate::index::HistoryIndex;
use crate::verify_fixture::{command, event, record, step};

#[test]
fn delay_and_connection_cut_evidence_pass_their_exact_contracts() {
    for mode in [Mode::Delay, Mode::Cut] {
        let index = HistoryIndex::build(&history(mode));
        let mut violations = Vec::new();
        crate::network_proxy::verify(&scenario(mode), &index, &mut violations);
        assert!(violations.is_empty(), "{mode:?}: {violations:?}");
    }
}

#[derive(Clone, Copy, Debug)]
enum Mode {
    Delay,
    Cut,
}

fn scenario(mode: Mode) -> Scenario {
    let steps = match mode {
        Mode::Delay => vec![
            step(
                "apply",
                ScenarioAction::AlterNetworkFault(delay("delay-apply", NetworkFaultState::Present)),
            ),
            step("during", send("during-delay")),
            step(
                "remove",
                ScenarioAction::AlterNetworkFault(delay("delay-remove", NetworkFaultState::Absent)),
            ),
            step("after", send("after-delay")),
        ],
        Mode::Cut => vec![
            step(
                "cut",
                ScenarioAction::CutNetworkConnections(NetworkConnectionCutAction {
                    operation_id: environment_id("cut-live"),
                    broker_ordinal: 1,
                    timeout_ms: 1_000,
                }),
            ),
            step("after", send("after-cut")),
        ],
    };
    Scenario {
        schema_version: testlab_schema::SCENARIO_SCHEMA_VERSION,
        id: ScenarioId::new(match mode {
            Mode::Delay => "network.delay",
            Mode::Cut => "network.cut",
        })
        .unwrap_or_else(|error| panic!("scenario id: {error}")),
        title: "network mode".to_owned(),
        description: "network mode verifier fixture".to_owned(),
        timeout_ms: 10_000,
        requires: BTreeSet::new(),
        steps,
        assertions: Vec::new(),
    }
}

fn history(mode: Mode) -> Vec<HistoryEntry> {
    match mode {
        Mode::Delay => vec![
            control(
                0,
                NetworkProxyControl::AlterFault(delay("delay-apply", NetworkFaultState::Present)),
            ),
            send_command(1, "during-delay"),
            terminal(2, "during-delay"),
            control(
                3,
                NetworkProxyControl::AlterFault(delay("delay-remove", NetworkFaultState::Absent)),
            ),
            delay_observation(4),
            send_command(5, "after-delay"),
            terminal(6, "after-delay"),
            proxy_process(7),
        ],
        Mode::Cut => vec![
            control(
                0,
                NetworkProxyControl::CutConnections(NetworkConnectionCutAction {
                    operation_id: environment_id("cut-live"),
                    broker_ordinal: 1,
                    timeout_ms: 1_000,
                }),
            ),
            cut_observation(1),
            send_command(2, "after-cut"),
            terminal(3, "after-cut"),
            proxy_process(4),
        ],
    }
}

fn delay(id: &str, state: NetworkFaultState) -> NetworkFaultAction {
    NetworkFaultAction {
        operation_id: environment_id(id),
        broker_ordinal: 1,
        fault: NetworkFault::Delay {
            direction: NetworkDirection::ClientToBroker,
            delay_ms: 25,
        },
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

fn send_command(sequence: u64, id: &str) -> HistoryEntry {
    command(
        sequence,
        AdapterCommand::Send {
            producer_id: producer(),
            operation_id: operation(id),
            record: record(id),
        },
    )
}

fn terminal(sequence: u64, id: &str) -> HistoryEntry {
    event(
        sequence,
        AdapterEvent::OperationTerminal {
            operation_id: operation(id),
            status: TerminalStatus::Acknowledged,
            code: None,
            offset: None,
        },
    )
}

fn control(sequence: u64, control: NetworkProxyControl) -> HistoryEntry {
    history_entry(sequence, HistoryPayload::NetworkProxyControl { control })
}

fn delay_observation(sequence: u64) -> HistoryEntry {
    let observation = NetworkProxyObservation::FaultWindow(NetworkFaultWindowObservation {
        observation: 0,
        apply_operation_id: environment_id("delay-apply"),
        remove_operation_id: environment_id("delay-remove"),
        broker_ordinal: 1,
        fault: NetworkFault::Delay {
            direction: NetworkDirection::ClientToBroker,
            delay_ms: 25,
        },
        started_unix_ms: 1,
        completed_unix_ms: 3,
        connections_at_start: 1,
        connections_accepted: 0,
        client_to_broker_bytes: 7,
        broker_to_client_bytes: 0,
        delayed_client_to_broker_bytes: 7,
        delayed_broker_to_client_bytes: 0,
        blocked_intervals: 0,
    });
    history_entry(
        sequence,
        HistoryPayload::NetworkProxyObservation { observation },
    )
}

fn cut_observation(sequence: u64) -> HistoryEntry {
    let observation = NetworkProxyObservation::ConnectionsCut(NetworkConnectionsCutObservation {
        observation: 0,
        operation_id: environment_id("cut-live"),
        broker_ordinal: 1,
        connections_cut: 1,
        completed_unix_ms: 1,
    });
    history_entry(
        sequence,
        HistoryPayload::NetworkProxyObservation { observation },
    )
}

fn proxy_process(sequence: u64) -> HistoryEntry {
    history_entry(
        sequence,
        HistoryPayload::EnvironmentOperation {
            operation: EnvironmentOperation {
                id: environment_id("network-proxy-process"),
                kind: EnvironmentOperationKind::NetworkProxy,
                program: "/tmp/testctl".to_owned(),
                args: vec!["network-proxy-worker".to_owned()],
                started_unix_ms: 0,
                completed_unix_ms: sequence,
                status: EnvironmentOperationStatus::Succeeded,
                exit_code: Some(0),
                stdout_artifact: Some("network-proxy.jsonl".to_owned()),
                stderr_artifact: Some("network-proxy.stderr.txt".to_owned()),
                diagnostic: None,
            },
        },
    )
}

fn history_entry(sequence: u64, payload: HistoryPayload) -> HistoryEntry {
    HistoryEntry {
        sequence,
        observed_unix_ms: sequence,
        payload,
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
