//! Transaction leader-replacement tests pin exact commands and completion windows.

use std::collections::BTreeSet;

use testlab_schema::{
    AdapterCommand, AdapterEvent, BatchRecord, BrokerRoleTarget, ByteString, HistoryEntry,
    OperationId, ProducerId, RecordSpec, Scenario, ScenarioAction, ScenarioId, TerminalStatus,
    TransactionDisposition, TransactionSendMethod,
};

use crate::index::HistoryIndex;
use crate::verify_fixture::{command, event, step};

#[test]
fn exact_transaction_inside_replacement_window_passes() {
    assert!(!has(
        &violations(vec![
            command(2, transaction_command()),
            accepted(3, "leader-transaction-1"),
            staged(4, "leader-transaction-1"),
            accepted(5, "leader-transaction-2"),
            staged(6, "leader-transaction-2"),
            completed(7, TransactionDisposition::Commit),
        ]),
        "FAULT-005"
    ));
}

#[test]
fn pre_election_transaction_command_fails() {
    let violations = violations(vec![
        command(0, transaction_command()),
        accepted(3, "leader-transaction-1"),
        staged(4, "leader-transaction-1"),
        accepted(5, "leader-transaction-2"),
        staged(6, "leader-transaction-2"),
        completed(7, TransactionDisposition::Commit),
    ]);
    assert!(has(&violations, "FAULT-005"), "{violations:?}");
}

#[test]
fn incomplete_transaction_staging_fails() {
    let violations = violations(vec![
        command(2, transaction_command()),
        accepted(3, "leader-transaction-1"),
        staged(4, "leader-transaction-1"),
        accepted(5, "leader-transaction-2"),
        completed(6, TransactionDisposition::Commit),
    ]);
    assert!(has(&violations, "FAULT-005"), "{violations:?}");
}

#[test]
fn wrong_transaction_disposition_fails() {
    let violations = violations(vec![
        command(2, transaction_command()),
        accepted(3, "leader-transaction-1"),
        staged(4, "leader-transaction-1"),
        accepted(5, "leader-transaction-2"),
        staged(6, "leader-transaction-2"),
        completed(7, TransactionDisposition::Abort),
    ]);
    assert!(has(&violations, "FAULT-005"), "{violations:?}");
}

#[test]
fn duplicate_transaction_command_fails() {
    let violations = violations(vec![
        command(2, transaction_command()),
        command(3, transaction_command()),
        accepted(4, "leader-transaction-1"),
        staged(5, "leader-transaction-1"),
        accepted(6, "leader-transaction-2"),
        staged(7, "leader-transaction-2"),
        completed(8, TransactionDisposition::Commit),
    ]);
    assert!(has(&violations, "FAULT-005"), "{violations:?}");
}

#[test]
fn completion_after_restore_fails() {
    let violations = violations(vec![
        command(2, transaction_command()),
        accepted(3, "leader-transaction-1"),
        staged(4, "leader-transaction-1"),
        accepted(5, "leader-transaction-2"),
        staged(6, "leader-transaction-2"),
        completed(10, TransactionDisposition::Commit),
    ]);
    assert!(has(&violations, "FAULT-005"), "{violations:?}");
}

#[test]
fn legacy_event_only_fixture_is_skipped() {
    assert!(!has(
        &violations(vec![
            accepted(3, "leader-transaction-1"),
            staged(4, "leader-transaction-1"),
            accepted(5, "leader-transaction-2"),
            staged(6, "leader-transaction-2"),
            completed(7, TransactionDisposition::Commit),
        ]),
        "FAULT-005"
    ));
}

fn violations(history: Vec<HistoryEntry>) -> Vec<testlab_schema::Violation> {
    let index = HistoryIndex::build(&history);
    let mut violations = Vec::new();
    super::verify(&scenario(), 0, &target(), &index, 1, 9, &mut violations);
    violations
}

fn scenario() -> Scenario {
    Scenario {
        schema_version: testlab_schema::SCENARIO_SCHEMA_VERSION,
        id: ScenarioId::new("fault.transaction-leader-recovery")
            .unwrap_or_else(|error| panic!("scenario id: {error}")),
        title: "transaction leader recovery".to_owned(),
        description: "transaction leader recovery fixture".to_owned(),
        timeout_ms: 10_000,
        requires: BTreeSet::new(),
        steps: vec![
            step(
                "stop",
                ScenarioAction::StopBrokerRole {
                    target: target(),
                    timeout_ms: 1_000,
                },
            ),
            step("transaction", transaction_action()),
            step(
                "restore",
                ScenarioAction::RestoreBrokerRole {
                    target: target(),
                    timeout_ms: 1_000,
                },
            ),
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
    ["leader-transaction-1", "leader-transaction-2"]
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

fn target() -> BrokerRoleTarget {
    BrokerRoleTarget::PartitionLeader {
        topic: "records".to_owned(),
        partition: 0,
    }
}

fn operation(value: &str) -> OperationId {
    OperationId::new(value).unwrap_or_else(|error| panic!("operation id: {error}"))
}

fn transaction() -> OperationId {
    operation("leader-transaction")
}

fn producer() -> ProducerId {
    ProducerId::new("transactional-producer").unwrap_or_else(|error| panic!("producer id: {error}"))
}

fn has(violations: &[testlab_schema::Violation], contract: &str) -> bool {
    violations
        .iter()
        .any(|violation| violation.contract_id.as_str() == contract)
}
