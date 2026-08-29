//! Assigned cursor tests pin replay and successive record order to independent broker truth.

use testlab_schema::{
    AdapterEvent, BrokerObservation, ByteString, ConsumedRecord, ConsumerId, HistoryEntry,
    OperationId, Scenario, ScenarioAction, TerminalStatus, VisibilityExpectation,
};

use crate::index::HistoryIndex;
use crate::verify_fixture::{event, record, scenario, step};

#[test]
fn beginning_reset_can_replay_one_independently_observed_record() {
    let mut scenario = base_scenario();
    for receive_id in ["receive-first", "receive-replay"] {
        scenario.steps.push(receive_step(receive_id, "op-1"));
    }
    let history = [
        receive_event(0, "receive-first", "value", 0),
        receive_event(1, "receive-replay", "value", 0),
    ];

    let violations = verify(&scenario, &history, &[observed("op-1", "value", 0, 0)]);

    assert!(!violates(&violations, "CONS-012"));
}

#[test]
fn successive_direct_receives_cannot_swap_broker_records() {
    let mut scenario = base_scenario();
    scenario.steps.insert(
        4,
        step(
            "send-2",
            ScenarioAction::Send {
                producer_id: testlab_schema::ProducerId::new("producer-1")
                    .unwrap_or_else(|error| panic!("producer id: {error}")),
                operation_id: operation("op-2"),
                record: record("second"),
            },
        ),
    );
    scenario.steps.extend([
        receive_step("receive-first", "op-1"),
        receive_step("receive-second", "op-2"),
    ]);
    let history = [
        receive_event(0, "receive-first", "second", 1),
        receive_event(1, "receive-second", "value", 0),
    ];
    let observations = [
        observed("op-1", "value", 0, 0),
        observed("op-2", "second", 1, 1),
    ];

    let violations = verify(&scenario, &history, &observations);

    assert!(violates(&violations, "CONS-012"));
}

fn receive_step(id: &str, expected: &str) -> testlab_schema::ScenarioStep {
    step(
        id,
        ScenarioAction::Receive {
            consumer_id: consumer("assigned-1"),
            receive_id: operation(id),
            expected_operation_id: operation(expected),
            timeout_ms: 1_000,
        },
    )
}

fn receive_event(sequence: u64, id: &str, value: &str, offset: i64) -> HistoryEntry {
    event(
        sequence,
        AdapterEvent::ReceiveCompleted {
            receive_id: operation(id),
            records: vec![ConsumedRecord {
                topic: "records".to_owned(),
                partition: 0,
                offset,
                timestamp_millis: None,
                key: None,
                value: Some(ByteString::utf8(value)),
                headers: Vec::new(),
            }],
        },
    )
}

fn observed(operation_id: &str, value: &str, offset: i64, ordinal: u64) -> BrokerObservation {
    let record = record(value);
    let digest = record
        .digest()
        .unwrap_or_else(|error| panic!("record digest: {error}"));
    BrokerObservation {
        observation: ordinal,
        offset,
        operation_id: operation(operation_id),
        record,
        digest,
    }
}

fn verify(
    scenario: &Scenario,
    history: &[HistoryEntry],
    observations: &[BrokerObservation],
) -> Vec<testlab_schema::Violation> {
    let index = HistoryIndex::build(history);
    let mut violations = Vec::new();
    crate::record_offsets::verify(scenario, &index, observations, &mut violations);
    violations
}

fn base_scenario() -> Scenario {
    scenario(
        TerminalStatus::Acknowledged,
        VisibilityExpectation::ExactlyOnce,
    )
}

fn violates(violations: &[testlab_schema::Violation], contract: &str) -> bool {
    violations
        .iter()
        .any(|violation| violation.contract_id.as_str() == contract)
}

fn consumer(value: &str) -> ConsumerId {
    ConsumerId::new(value).unwrap_or_else(|error| panic!("consumer id: {error}"))
}

fn operation(value: &str) -> OperationId {
    OperationId::new(value).unwrap_or_else(|error| panic!("operation id: {error}"))
}
