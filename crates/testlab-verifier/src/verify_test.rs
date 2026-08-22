//! Deterministic verifier tests cover valid admission and delivery truths.

use testlab_schema::{
    AdapterEvent, ByteString, Capability, ConsumedRecord, ConsumerId, OperationId, ScenarioAction,
    TerminalStatus, VisibilityExpectation,
};

use super::verify;
use crate::verify_fixture::{
    adapter, event, history, observation, rejected_history, scenario, step,
};

#[test]
fn explicit_admission_rejection_passes_without_a_terminal() {
    let mut scenario = scenario(
        TerminalStatus::DefinitelyNotSent,
        VisibilityExpectation::Absent,
    );
    scenario.assertions[0].accepted = false;
    scenario.assertions[0].terminal = None;

    let verdict = verify(&scenario, &adapter(), &rejected_history(), &[]);

    assert!(verdict.is_passed());
}

#[test]
fn acknowledged_exact_record_passes() {
    let verdict = verify(
        &scenario(
            TerminalStatus::Acknowledged,
            VisibilityExpectation::ExactlyOnce,
        ),
        &adapter(),
        &history(TerminalStatus::Acknowledged),
        &[observation(0, "value")],
    );

    assert!(verdict.is_passed());
}

#[test]
fn lost_response_preserves_possibly_sent_truth() {
    let verdict = verify(
        &scenario(
            TerminalStatus::PossiblySent,
            VisibilityExpectation::ExactlyOnce,
        ),
        &adapter(),
        &history(TerminalStatus::PossiblySent),
        &[observation(0, "value")],
    );

    assert!(verdict.is_passed());
}

#[test]
fn assigned_consumer_exact_round_trip_passes() {
    let mut scenario = scenario(
        TerminalStatus::Acknowledged,
        VisibilityExpectation::ExactlyOnce,
    );
    scenario.requires.insert(Capability::AssignedConsumer);
    let shutdown = scenario.steps.remove(6);
    let consumer = id(ConsumerId::new("consumer-1"));
    let receive = id(OperationId::new("receive-1"));
    scenario.steps.extend([
        step(
            "consumer",
            ScenarioAction::CreateAssignedConsumer {
                client_id: id(testlab_schema::ClientId::new("client-1")),
                consumer_id: consumer.clone(),
            },
        ),
        step(
            "assign",
            ScenarioAction::AssignBeginning {
                consumer_id: consumer.clone(),
                topic: "records".to_owned(),
                partition: 0,
            },
        ),
        step(
            "receive",
            ScenarioAction::Receive {
                consumer_id: consumer.clone(),
                receive_id: receive.clone(),
                expected_operation_id: id(OperationId::new("op-1")),
                timeout_ms: 1_000,
            },
        ),
        step(
            "consumer-close",
            ScenarioAction::CloseAssignedConsumer {
                consumer_id: consumer.clone(),
            },
        ),
    ]);
    scenario.steps.push(shutdown);
    let mut history = history(TerminalStatus::Acknowledged);
    history.truncate(8);
    history.extend([
        event(
            8,
            AdapterEvent::AssignedConsumerCreated {
                consumer_id: consumer.clone(),
            },
        ),
        event(
            9,
            AdapterEvent::AssignmentCompleted {
                consumer_id: consumer.clone(),
            },
        ),
        event(
            10,
            AdapterEvent::ReceiveCompleted {
                receive_id: receive,
                records: vec![ConsumedRecord {
                    topic: "records".to_owned(),
                    partition: 0,
                    offset: 0,
                    timestamp_millis: None,
                    key: None,
                    value: Some(ByteString::hex(b"value")),
                    headers: Vec::new(),
                }],
            },
        ),
        event(
            11,
            AdapterEvent::AssignedConsumerClosed {
                consumer_id: consumer,
            },
        ),
        event(
            12,
            AdapterEvent::ClientShutdown {
                client_id: id(testlab_schema::ClientId::new("client-1")),
            },
        ),
        event(13, AdapterEvent::Finished),
    ]);

    let verdict = verify(&scenario, &adapter(), &history, &[observation(0, "value")]);

    assert!(verdict.is_passed(), "{verdict:?}");
}

#[test]
fn classic_group_exact_round_trip_requires_commit() {
    let mut scenario = scenario(
        TerminalStatus::Acknowledged,
        VisibilityExpectation::ExactlyOnce,
    );
    scenario.requires.insert(Capability::ConsumerGroups);
    let shutdown = scenario.steps.remove(6);
    let consumer = id(ConsumerId::new("group-consumer-1"));
    let receive = id(OperationId::new("group-receive-1"));
    scenario.steps.extend([
        step(
            "group-consumer",
            ScenarioAction::CreateGroupConsumer {
                client_id: id(testlab_schema::ClientId::new("client-1")),
                consumer_id: consumer.clone(),
                group_id: "group-1".to_owned(),
                topic: "records".to_owned(),
            },
        ),
        step(
            "group-receive",
            ScenarioAction::GroupReceive {
                consumer_id: consumer.clone(),
                receive_id: receive.clone(),
                expected_operation_id: id(OperationId::new("op-1")),
                timeout_ms: 1_000,
            },
        ),
        step(
            "group-close",
            ScenarioAction::CloseGroupConsumer {
                consumer_id: consumer.clone(),
            },
        ),
    ]);
    scenario.steps.push(shutdown);
    let mut events = history(TerminalStatus::Acknowledged);
    events.truncate(8);
    events.extend([
        event(
            8,
            AdapterEvent::GroupConsumerCreated {
                consumer_id: consumer.clone(),
            },
        ),
        event(
            9,
            AdapterEvent::GroupReceiveCompleted {
                receive_id: receive,
                records: vec![ConsumedRecord {
                    topic: "records".to_owned(),
                    partition: 0,
                    offset: 0,
                    timestamp_millis: None,
                    key: None,
                    value: Some(ByteString::hex(b"value")),
                    headers: Vec::new(),
                }],
                committed: true,
            },
        ),
        event(
            10,
            AdapterEvent::GroupConsumerClosed {
                consumer_id: consumer,
            },
        ),
        event(
            11,
            AdapterEvent::ClientShutdown {
                client_id: id(testlab_schema::ClientId::new("client-1")),
            },
        ),
        event(12, AdapterEvent::Finished),
    ]);

    let verdict = verify(&scenario, &adapter(), &events, &[observation(0, "value")]);
    assert!(verdict.is_passed(), "{verdict:?}");

    if let testlab_schema::HistoryPayload::AdapterEvent { event } = &mut events[9].payload
        && let AdapterEvent::GroupReceiveCompleted { committed, .. } = &mut event.event
    {
        *committed = false;
    }
    let verdict = verify(&scenario, &adapter(), &events, &[observation(0, "value")]);
    assert!(
        verdict
            .violations
            .iter()
            .any(|violation| violation.contract_id.as_str() == "CONS-003"),
        "{verdict:?}"
    );
}

fn id<T, E>(result: Result<T, E>) -> T
where
    E: std::fmt::Display,
{
    result.unwrap_or_else(|error| panic!("fixture id: {error}"))
}
