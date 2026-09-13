//! Network producer recovery tests bind exact single and batch commands to terminals.

use std::collections::BTreeSet;

use testlab_schema::{
    AdapterCommand, AdapterEvent, BatchRecord, ByteString, EnvironmentOperationId, HeaderSpec,
    HistoryEntry, HistoryPayload, NetworkConnectionCutAction, NetworkProxyControl, OperationId,
    ProducerId, RecordSpec, Scenario, ScenarioAction, ScenarioId, TerminalStatus,
};

use crate::index::HistoryIndex;
use crate::verify_fixture::{command, event, step};

#[test]
fn exact_post_cut_batch_progress_passes() {
    let action = batch_action();
    let history = vec![
        control(0),
        command(1, batch_command()),
        terminal(2, "batch-after-cut-1"),
        terminal(3, "batch-after-cut-2"),
    ];

    assert!(!has(&violations(action, history), "NET-004"));
}

#[test]
fn missing_batch_terminal_fails_recovery() {
    let history = vec![
        control(0),
        command(1, batch_command()),
        terminal(2, "batch-after-cut-1"),
    ];

    let violations = violations(batch_action(), history);
    assert!(has(&violations, "NET-004"), "{violations:?}");
}

#[test]
fn duplicate_batch_command_fails_recovery() {
    let history = vec![
        control(0),
        command(1, batch_command()),
        command(2, batch_command()),
        terminal(3, "batch-after-cut-1"),
        terminal(4, "batch-after-cut-2"),
    ];

    let violations = violations(batch_action(), history);
    assert!(has(&violations, "NET-004"), "{violations:?}");
}

#[test]
fn pre_cut_single_command_with_post_cut_terminal_fails_recovery() {
    let history = vec![
        command(0, single_command()),
        control(1),
        terminal(2, "single-after-cut"),
    ];

    let violations = violations(single_action(), history);
    assert!(has(&violations, "NET-004"), "{violations:?}");
}

#[test]
fn exact_post_cut_single_command_still_passes() {
    let history = vec![
        control(0),
        command(1, single_command()),
        terminal(2, "single-after-cut"),
    ];

    assert!(!has(&violations(single_action(), history), "NET-004"));
}

fn violations(
    recovery: ScenarioAction,
    history: Vec<HistoryEntry>,
) -> Vec<testlab_schema::Violation> {
    let scenario = Scenario {
        schema_version: testlab_schema::SCENARIO_SCHEMA_VERSION,
        id: ScenarioId::new("network.producer-batch-recovery")
            .unwrap_or_else(|error| panic!("scenario id: {error}")),
        title: "producer batch recovery".to_owned(),
        description: "producer batch recovery fixture".to_owned(),
        timeout_ms: 10_000,
        requires: BTreeSet::new(),
        steps: vec![
            step("cut", ScenarioAction::CutNetworkConnections(cut_action())),
            step("recover", recovery),
        ],
        assertions: Vec::new(),
    };
    let index = HistoryIndex::build(&history);
    let mut violations = Vec::new();
    crate::network_proxy_progress::verify(&scenario, &index, &mut violations);
    violations
}

fn batch_action() -> ScenarioAction {
    ScenarioAction::SendBatch {
        producer_id: producer(),
        operations: batch_records(),
    }
}

fn batch_command() -> AdapterCommand {
    AdapterCommand::SendBatch {
        producer_id: producer(),
        operations: batch_records(),
    }
}

fn single_action() -> ScenarioAction {
    ScenarioAction::Send {
        producer_id: producer(),
        operation_id: operation("single-after-cut"),
        method: Default::default(),
        partitioning: Default::default(),
        topic_identity_operation_id: None,
        record: record("single-after-cut", 1),
    }
}

fn single_command() -> AdapterCommand {
    AdapterCommand::Send {
        producer_id: producer(),
        operation_id: operation("single-after-cut"),
        method: Default::default(),
        partitioning: Default::default(),
        validate_topic_uuid: false,
        record: record("single-after-cut", 1),
    }
}

fn batch_records() -> Vec<BatchRecord> {
    vec![
        BatchRecord {
            operation_id: operation("batch-after-cut-1"),
            record: record("batch-after-cut-1", 1),
        },
        BatchRecord {
            operation_id: operation("batch-after-cut-2"),
            record: record("batch-after-cut-2", 2),
        },
    ]
}

fn record(id: &str, sequence: u64) -> RecordSpec {
    RecordSpec {
        topic: "records".to_owned(),
        partition: 0,
        sequence,
        timestamp_millis: None,
        key: None,
        value: Some(ByteString::utf8(id)),
        headers: vec![
            HeaderSpec {
                name: "testlab-operation-id".to_owned(),
                value: Some(ByteString::utf8(id)),
            },
            HeaderSpec {
                name: "testlab-sequence".to_owned(),
                value: Some(ByteString::utf8(sequence.to_string())),
            },
        ],
    }
}

fn terminal(sequence: u64, id: &str) -> HistoryEntry {
    event(
        sequence,
        AdapterEvent::OperationTerminal {
            operation_id: operation(id),
            status: TerminalStatus::Acknowledged,
            code: None,
            partition: Some(0),
            offset: Some(i64::try_from(sequence).unwrap_or(i64::MAX)),
            timestamp_millis: None,
            receipt: None,
        },
    )
}

fn control(sequence: u64) -> HistoryEntry {
    HistoryEntry {
        sequence,
        observed_unix_ms: sequence,
        payload: HistoryPayload::NetworkProxyControl {
            control: NetworkProxyControl::CutConnections(cut_action()),
        },
    }
}

fn cut_action() -> NetworkConnectionCutAction {
    NetworkConnectionCutAction {
        operation_id: EnvironmentOperationId::new("cut-connections")
            .unwrap_or_else(|error| panic!("environment id: {error}")),
        broker_ordinal: 1,
        timeout_ms: 1_000,
    }
}

fn operation(value: &str) -> OperationId {
    OperationId::new(value).unwrap_or_else(|error| panic!("operation id: {error}"))
}

fn producer() -> ProducerId {
    ProducerId::new("producer-1").unwrap_or_else(|error| panic!("producer id: {error}"))
}

fn has(violations: &[testlab_schema::Violation], contract: &str) -> bool {
    violations
        .iter()
        .any(|violation| violation.contract_id.as_str() == contract)
}
