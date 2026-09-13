//! Network transaction recovery tests pin the post-control execution window.

use std::collections::BTreeSet;

use testlab_schema::{
    AdapterCommand, AdapterEvent, BatchRecord, ByteString, EnvironmentOperationId, HistoryEntry,
    HistoryPayload, NetworkConnectionCutAction, NetworkProxyControl, OperationId, ProducerId,
    RecordSpec, Scenario, ScenarioAction, ScenarioId, TerminalStatus, TransactionDisposition,
    TransactionSendMethod,
};

use crate::index::HistoryIndex;
use crate::verify_fixture::{command, event, step};

#[test]
fn exact_transaction_after_cut_passes() {
    assert!(!has(
        &violations(vec![
            control(0),
            command(1, transaction_command()),
            accepted(2, "network-transaction-1"),
            staged(3, "network-transaction-1"),
            accepted(4, "network-transaction-2"),
            staged(5, "network-transaction-2"),
            completed(6, TransactionDisposition::Commit),
        ]),
        "NET-006"
    ));
}

#[test]
fn pre_cut_transaction_command_fails() {
    let violations = violations(vec![
        command(0, transaction_command()),
        control(1),
        accepted(2, "network-transaction-1"),
        staged(3, "network-transaction-1"),
        accepted(4, "network-transaction-2"),
        staged(5, "network-transaction-2"),
        completed(6, TransactionDisposition::Commit),
    ]);
    assert!(has(&violations, "NET-006"), "{violations:?}");
}

#[test]
fn incomplete_staging_after_cut_fails() {
    let violations = violations(vec![
        control(0),
        command(1, transaction_command()),
        accepted(2, "network-transaction-1"),
        staged(3, "network-transaction-1"),
        accepted(4, "network-transaction-2"),
        completed(5, TransactionDisposition::Commit),
    ]);
    assert!(has(&violations, "NET-006"), "{violations:?}");
}

#[test]
fn duplicate_transaction_command_after_cut_fails() {
    let violations = violations(vec![
        control(0),
        command(1, transaction_command()),
        command(2, transaction_command()),
        accepted(3, "network-transaction-1"),
        staged(4, "network-transaction-1"),
        accepted(5, "network-transaction-2"),
        staged(6, "network-transaction-2"),
        completed(7, TransactionDisposition::Commit),
    ]);
    assert!(has(&violations, "NET-006"), "{violations:?}");
}

#[test]
fn wrong_transaction_disposition_after_cut_fails() {
    let violations = violations(vec![
        control(0),
        command(1, transaction_command()),
        accepted(2, "network-transaction-1"),
        staged(3, "network-transaction-1"),
        accepted(4, "network-transaction-2"),
        staged(5, "network-transaction-2"),
        completed(6, TransactionDisposition::Abort),
    ]);
    assert!(has(&violations, "NET-006"), "{violations:?}");
}

#[test]
fn completion_after_next_network_control_fails() {
    let mut scenario = scenario();
    scenario.steps.push(step(
        "next-cut",
        ScenarioAction::CutNetworkConnections(next_cut_action()),
    ));
    let history = vec![
        control(0),
        command(1, transaction_command()),
        accepted(2, "network-transaction-1"),
        staged(3, "network-transaction-1"),
        accepted(4, "network-transaction-2"),
        staged(5, "network-transaction-2"),
        control_for(6, next_cut_action()),
        completed(7, TransactionDisposition::Commit),
    ];
    let violations = violations_for(&scenario, &history);
    assert!(has(&violations, "NET-006"), "{violations:?}");
}

#[test]
fn legacy_event_only_fixture_is_skipped() {
    assert!(!has(
        &violations(vec![
            control(0),
            accepted(1, "network-transaction-1"),
            staged(2, "network-transaction-1"),
            accepted(3, "network-transaction-2"),
            staged(4, "network-transaction-2"),
            completed(5, TransactionDisposition::Commit),
        ]),
        "NET-006"
    ));
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "the test helper owns each constructed history fixture"
)]
fn violations(history: Vec<HistoryEntry>) -> Vec<testlab_schema::Violation> {
    violations_for(&scenario(), &history)
}

fn violations_for(scenario: &Scenario, history: &[HistoryEntry]) -> Vec<testlab_schema::Violation> {
    let index = HistoryIndex::build(history);
    let mut violations = Vec::new();
    super::verify(scenario, &index, &mut violations);
    violations
}

fn scenario() -> Scenario {
    Scenario {
        schema_version: testlab_schema::SCENARIO_SCHEMA_VERSION,
        id: ScenarioId::new("network.transaction-recovery")
            .unwrap_or_else(|error| panic!("scenario id: {error}")),
        title: "transaction recovery".to_owned(),
        description: "transaction recovery fixture".to_owned(),
        timeout_ms: 10_000,
        requires: BTreeSet::new(),
        steps: vec![
            step("cut", ScenarioAction::CutNetworkConnections(cut_action())),
            step("transaction", transaction_action()),
        ],
        assertions: Vec::new(),
    }
}

fn transaction_action() -> ScenarioAction {
    ScenarioAction::ExecuteTransaction {
        producer_id: producer(),
        transaction_id: transaction(),
        operations: operations(),
        method: TransactionSendMethod::SendBatch,
        disposition: TransactionDisposition::Commit,
        topic_identity_operation_id: None,
        timeout_ms: 1_000,
    }
}

fn transaction_command() -> AdapterCommand {
    AdapterCommand::ExecuteTransaction {
        producer_id: producer(),
        transaction_id: transaction(),
        operations: operations(),
        method: TransactionSendMethod::SendBatch,
        disposition: TransactionDisposition::Commit,
        validate_topic_uuids: false,
        timeout_ms: 1_000,
    }
}

fn operations() -> Vec<BatchRecord> {
    ["network-transaction-1", "network-transaction-2"]
        .into_iter()
        .enumerate()
        .map(|(index, value)| BatchRecord {
            operation_id: operation(value),
            record: RecordSpec {
                topic: "records".to_owned(),
                partition: 0,
                sequence: u64::try_from(index + 1).unwrap_or(u64::MAX),
                timestamp_millis: None,
                key: None,
                value: Some(ByteString::utf8(value)),
                headers: Vec::new(),
            },
        })
        .collect()
}

fn accepted(sequence: u64, id: &str) -> HistoryEntry {
    event(
        sequence,
        AdapterEvent::OperationAccepted {
            operation_id: operation(id),
        },
    )
}

fn staged(sequence: u64, id: &str) -> HistoryEntry {
    event(
        sequence,
        AdapterEvent::OperationTerminal {
            operation_id: operation(id),
            status: TerminalStatus::TransactionStaged,
            code: None,
            partition: Some(0),
            offset: Some(i64::try_from(sequence).unwrap_or(i64::MAX)),
            timestamp_millis: None,
            receipt: None,
        },
    )
}

fn completed(sequence: u64, disposition: TransactionDisposition) -> HistoryEntry {
    event(
        sequence,
        AdapterEvent::TransactionCompleted {
            transaction_id: transaction(),
            disposition,
            validated_topic_ids: Vec::new(),
        },
    )
}

fn control(sequence: u64) -> HistoryEntry {
    control_for(sequence, cut_action())
}

fn control_for(sequence: u64, action: NetworkConnectionCutAction) -> HistoryEntry {
    HistoryEntry {
        sequence,
        observed_unix_ms: sequence,
        payload: HistoryPayload::NetworkProxyControl {
            control: NetworkProxyControl::CutConnections(action),
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

fn next_cut_action() -> NetworkConnectionCutAction {
    NetworkConnectionCutAction {
        operation_id: EnvironmentOperationId::new("next-cut-connections")
            .unwrap_or_else(|error| panic!("environment id: {error}")),
        broker_ordinal: 2,
        timeout_ms: 1_000,
    }
}

fn operation(value: &str) -> OperationId {
    OperationId::new(value).unwrap_or_else(|error| panic!("operation id: {error}"))
}

fn transaction() -> OperationId {
    operation("network-transaction")
}

fn producer() -> ProducerId {
    ProducerId::new("transactional-producer-1")
        .unwrap_or_else(|error| panic!("producer id: {error}"))
}

fn has(violations: &[testlab_schema::Violation], contract: &str) -> bool {
    violations
        .iter()
        .any(|violation| violation.contract_id.as_str() == contract)
}
