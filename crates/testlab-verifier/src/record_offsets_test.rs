//! Record offset tests pin public metadata, consumer coordinates, and partition order.

use testlab_schema::{
    AdapterEvent, BatchRecord, BrokerObservation, ConsumedRecord, ConsumerId, HistoryEntry,
    OperationId, ScenarioAction, TerminalStatus, VisibilityExpectation,
};

use crate::index::HistoryIndex;
use crate::verify_fixture::{event, history, observation, record, scenario, step};

#[test]
fn exact_terminal_offset_matches_independent_observation() {
    let scenario = scenario(
        TerminalStatus::Acknowledged,
        VisibilityExpectation::ExactlyOnce,
    );
    let history = history(TerminalStatus::Acknowledged);
    let observations = [observation(0, "value")];

    let violations = verify_offsets(&scenario, &history, &observations);

    assert!(!violates(&violations, "PROD-010"));
}

#[test]
fn mismatched_terminal_offset_fails_public_offset_contract() {
    let scenario = scenario(
        TerminalStatus::Acknowledged,
        VisibilityExpectation::ExactlyOnce,
    );
    let history = history(TerminalStatus::Acknowledged);
    let observations = [observation(1, "value")];

    let violations = verify_offsets(&scenario, &history, &observations);

    assert!(violates(&violations, "PROD-010"));
}

#[test]
fn uncertain_terminal_with_offset_fails_public_offset_contract() {
    let scenario = scenario(
        TerminalStatus::PossiblySent,
        VisibilityExpectation::ZeroOrOne,
    );
    let history = [event(
        0,
        AdapterEvent::OperationTerminal {
            operation_id: id(OperationId::new("op-1")),
            status: TerminalStatus::PossiblySent,
            code: None,
            offset: Some(3),
        },
    )];

    let violations = verify_offsets(&scenario, &history, &[]);

    assert!(violates(&violations, "PROD-010"));
}

#[test]
fn reversed_same_partition_offsets_fail_declared_order() {
    let mut scenario = scenario(
        TerminalStatus::Acknowledged,
        VisibilityExpectation::ExactlyOnce,
    );
    scenario.steps.insert(
        4,
        step(
            "send-2",
            ScenarioAction::Send {
                producer_id: id(testlab_schema::ProducerId::new("producer-1")),
                operation_id: id(OperationId::new("op-2")),
                record: record("second"),
            },
        ),
    );
    let observations = [
        observed("op-1", "value", 1, 0),
        observed("op-2", "second", 0, 1),
    ];

    let violations = verify_offsets(&scenario, &[], &observations);

    assert!(violates(&violations, "PROD-011"));
}

#[test]
fn reversed_batch_offsets_fail_caller_order() {
    let mut scenario = scenario(
        TerminalStatus::Acknowledged,
        VisibilityExpectation::ExactlyOnce,
    );
    scenario.steps[3] = step(
        "batch",
        ScenarioAction::SendBatch {
            producer_id: id(testlab_schema::ProducerId::new("producer-1")),
            operations: vec![
                BatchRecord {
                    operation_id: id(OperationId::new("op-1")),
                    record: record("value"),
                },
                BatchRecord {
                    operation_id: id(OperationId::new("op-2")),
                    record: record("second"),
                },
            ],
        },
    );
    let observations = [
        observed("op-1", "value", 1, 0),
        observed("op-2", "second", 0, 1),
    ];

    let violations = verify_offsets(&scenario, &[], &observations);

    assert!(violates(&violations, "PROD-011"));
}

#[test]
fn mismatched_consumer_offset_fails_public_receive_contract() {
    let mut scenario = scenario(
        TerminalStatus::Acknowledged,
        VisibilityExpectation::ExactlyOnce,
    );
    let receive_id = id(OperationId::new("receive-1"));
    scenario.steps.push(step(
        "receive",
        ScenarioAction::Receive {
            consumer_id: id(ConsumerId::new("consumer-1")),
            receive_id: receive_id.clone(),
            expected_operation_id: id(OperationId::new("op-1")),
            timeout_ms: 1_000,
        },
    ));
    let history = [event(
        0,
        AdapterEvent::ReceiveCompleted {
            receive_id,
            records: vec![ConsumedRecord {
                topic: "records".to_owned(),
                partition: 0,
                offset: 9,
                timestamp_millis: None,
                key: None,
                value: Some(testlab_schema::ByteString::utf8("value")),
                headers: Vec::new(),
            }],
        },
    )];
    let observations = [observation(0, "value")];

    let violations = verify_offsets(&scenario, &history, &observations);

    assert!(violates(&violations, "CONS-012"));
}

fn verify_offsets(
    scenario: &testlab_schema::Scenario,
    history: &[HistoryEntry],
    observations: &[BrokerObservation],
) -> Vec<testlab_schema::Violation> {
    let index = HistoryIndex::build(history);
    let mut violations = Vec::new();
    crate::record_offsets::verify(scenario, &index, observations, &mut violations);
    violations
}

fn observed(operation_id: &str, value: &str, offset: i64, observation: u64) -> BrokerObservation {
    let record = record(value);
    let digest = record
        .digest()
        .unwrap_or_else(|error| panic!("record digest: {error}"));
    BrokerObservation {
        observation,
        offset,
        operation_id: id(OperationId::new(operation_id)),
        record,
        digest,
    }
}

fn violates(violations: &[testlab_schema::Violation], contract: &str) -> bool {
    violations
        .iter()
        .any(|violation| violation.contract_id.as_str() == contract)
}

fn id<T, E>(result: Result<T, E>) -> T
where
    E: std::fmt::Display,
{
    result.unwrap_or_else(|error| panic!("fixture identity: {error}"))
}
