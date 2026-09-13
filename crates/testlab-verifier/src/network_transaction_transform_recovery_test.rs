//! Transactional transform recovery tests bind the complete post-control command.

use std::collections::BTreeSet;

use testlab_schema::{
    AdapterCommand, AdapterEvent, BatchRecord, ByteString, ConsumedRecord, ConsumerId,
    EnvironmentOperationId, GroupMembershipEpoch, HistoryEntry, HistoryPayload,
    NetworkConnectionCutAction, NetworkProxyControl, OperationId, ProducerId, RecordSpec, Scenario,
    ScenarioAction, ScenarioId, TerminalStatus, TransactionDisposition,
    TransactionalTransformAction, TransactionalTransformCommand, TransactionalTransformCompletion,
};

use crate::index::HistoryIndex;
use crate::verify_fixture::{command, event, step};

#[test]
fn exact_transform_after_cut_passes() {
    assert!(!has(
        &violations(vec![
            control(0),
            command(1, transform_command()),
            accepted(2),
            staged(3),
            completed(4, TransactionDisposition::Commit),
        ]),
        "NET-006"
    ));
}

#[test]
fn pre_cut_transform_command_fails() {
    let violations = violations(vec![
        command(0, transform_command()),
        control(1),
        accepted(2),
        staged(3),
        completed(4, TransactionDisposition::Commit),
    ]);
    assert!(has(&violations, "NET-006"), "{violations:?}");
}

#[test]
fn mismatched_transform_command_fails() {
    let mut wrong = transform_command();
    let AdapterCommand::ExecuteTransactionalTransform(transform) = &mut wrong else {
        panic!("transform command missing");
    };
    transform.timeout_ms += 1;
    let violations = violations(vec![
        control(0),
        command(1, wrong),
        accepted(2),
        staged(3),
        completed(4, TransactionDisposition::Commit),
    ]);
    assert!(has(&violations, "NET-006"), "{violations:?}");
}

#[test]
fn transform_without_staged_output_fails() {
    let violations = violations(vec![
        control(0),
        command(1, transform_command()),
        accepted(2),
        completed(3, TransactionDisposition::Commit),
    ]);
    assert!(has(&violations, "NET-006"), "{violations:?}");
}

#[test]
fn transform_with_wrong_disposition_fails() {
    let violations = violations(vec![
        control(0),
        command(1, transform_command()),
        accepted(2),
        staged(3),
        completed(4, TransactionDisposition::Abort),
    ]);
    assert!(has(&violations, "NET-006"), "{violations:?}");
}

fn violations(history: Vec<HistoryEntry>) -> Vec<testlab_schema::Violation> {
    let index = HistoryIndex::build(&history);
    let mut violations = Vec::new();
    super::verify(&scenario(), &index, &mut violations);
    violations
}

fn scenario() -> Scenario {
    Scenario {
        schema_version: testlab_schema::SCENARIO_SCHEMA_VERSION,
        id: ScenarioId::new("network.transaction-transform-recovery")
            .unwrap_or_else(|error| panic!("scenario id: {error}")),
        title: "transaction transform recovery".to_owned(),
        description: "transaction transform recovery fixture".to_owned(),
        timeout_ms: 10_000,
        requires: BTreeSet::new(),
        steps: vec![
            step("cut", ScenarioAction::CutNetworkConnections(cut_action())),
            step(
                "transform",
                ScenarioAction::ExecuteTransactionalTransform(transform_action()),
            ),
        ],
        assertions: Vec::new(),
    }
}

fn transform_action() -> TransactionalTransformAction {
    TransactionalTransformAction {
        producer_id: producer(),
        consumer_id: consumer(),
        transaction_id: transaction(),
        expected_input_operation_id: operation("transform-input"),
        operations: vec![output()],
        disposition: TransactionDisposition::Commit,
        timeout_ms: 1_000,
    }
}

fn transform_command() -> AdapterCommand {
    let action = transform_action();
    AdapterCommand::ExecuteTransactionalTransform(TransactionalTransformCommand {
        producer_id: action.producer_id,
        consumer_id: action.consumer_id,
        transaction_id: action.transaction_id,
        operations: action.operations,
        disposition: action.disposition,
        timeout_ms: action.timeout_ms,
    })
}

fn output() -> BatchRecord {
    BatchRecord {
        operation_id: operation("transform-output"),
        record: RecordSpec {
            topic: "output".to_owned(),
            partition: 0,
            sequence: 1,
            timestamp_millis: None,
            key: None,
            value: Some(ByteString::utf8("output")),
            headers: Vec::new(),
        },
    }
}

fn accepted(sequence: u64) -> HistoryEntry {
    event(
        sequence,
        AdapterEvent::OperationAccepted {
            operation_id: operation("transform-output"),
        },
    )
}

fn staged(sequence: u64) -> HistoryEntry {
    event(
        sequence,
        AdapterEvent::OperationTerminal {
            operation_id: operation("transform-output"),
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
        AdapterEvent::TransactionalTransformCompleted(TransactionalTransformCompletion {
            transaction_id: transaction(),
            disposition,
            consumer_id: consumer(),
            records: vec![ConsumedRecord {
                topic: "input".to_owned(),
                partition: 0,
                offset: 0,
                timestamp_millis: None,
                key: None,
                value: Some(ByteString::utf8("input")),
                headers: Vec::new(),
            }],
            group_id: "transform-group".to_owned(),
            topic: "input".to_owned(),
            partition: 0,
            next_offset: 1,
            group_epoch: GroupMembershipEpoch::Classic { generation_id: 1 },
        }),
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
        operation_id: EnvironmentOperationId::new("cut-transform-connections")
            .unwrap_or_else(|error| panic!("environment id: {error}")),
        broker_ordinal: 1,
        timeout_ms: 1_000,
    }
}

fn operation(value: &str) -> OperationId {
    OperationId::new(value).unwrap_or_else(|error| panic!("operation id: {error}"))
}

fn transaction() -> OperationId {
    operation("network-transform")
}

fn producer() -> ProducerId {
    ProducerId::new("transform-producer").unwrap_or_else(|error| panic!("producer id: {error}"))
}

fn consumer() -> ConsumerId {
    ConsumerId::new("transform-consumer").unwrap_or_else(|error| panic!("consumer id: {error}"))
}

fn has(violations: &[testlab_schema::Violation], contract: &str) -> bool {
    violations
        .iter()
        .any(|violation| violation.contract_id.as_str() == contract)
}
