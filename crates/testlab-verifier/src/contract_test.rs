//! Contract tests force each verifier family to emit its stable identifier.

use testlab_schema::{
    AdapterCommand, AdapterEvent, ClientId, HistoryPayload, OperationId, ProducerId,
    TerminalStatus, Verdict, VisibilityExpectation,
};

use super::verify;
use crate::verify_fixture::{adapter, command, event, history, observation, record, scenario};

type EventPredicate = fn(&AdapterEvent) -> bool;

#[test]
fn protocol_contracts_detect_duplicate_ready_and_unknown_observation() {
    let scenario = scenario(
        TerminalStatus::Acknowledged,
        VisibilityExpectation::ExactlyOnce,
    );
    let mut duplicate_ready = history(TerminalStatus::Acknowledged);
    duplicate_ready.insert(1, duplicate_ready[0].clone());
    let verdict = verify(
        &scenario,
        &adapter(),
        &duplicate_ready,
        &[observation(0, "value")],
    );
    assert_contract(&verdict, "PROTO-001");

    let mut unknown = observation(1, "value");
    unknown.operation_id = operation_id("op-unknown");
    let verdict = verify(
        &scenario,
        &adapter(),
        &history(TerminalStatus::Acknowledged),
        &[observation(0, "value"), unknown],
    );
    assert_contract(&verdict, "PROTO-002");
}

#[test]
fn public_client_failure_is_valid_semantic_evidence() {
    let scenario = scenario(
        TerminalStatus::Acknowledged,
        VisibilityExpectation::ExactlyOnce,
    );
    let mut events = history(TerminalStatus::Acknowledged);
    remove_event(&mut events, is_flush_completed);
    remove_event(&mut events, is_producer_closed);
    remove_event(&mut events, is_client_shutdown);
    remove_event(&mut events, is_finished);
    let client = client_id("client-1");
    let producer = producer_id("producer-1");
    events.extend([
        command(
            10,
            AdapterCommand::CreateClient {
                client_id: client.clone(),
            },
        ),
        command(
            11,
            AdapterCommand::AwaitClientReady {
                client_id: client.clone(),
            },
        ),
        command(
            12,
            AdapterCommand::CreateProducer {
                client_id: client,
                producer_id: producer.clone(),
            },
        ),
        command(
            13,
            AdapterCommand::Send {
                producer_id: producer.clone(),
                operation_id: operation_id("op-1"),
                record: record("value"),
            },
        ),
        command(
            14,
            AdapterCommand::Flush {
                producer_id: producer,
            },
        ),
    ]);
    events.push(event(
        15,
        AdapterEvent::CommandFailed {
            code: "backpressure".to_owned(),
            diagnostic: "flush contended".to_owned(),
        },
    ));

    let verdict = verify(&scenario, &adapter(), &events, &[observation(0, "value")]);

    assert_contract(&verdict, "CLIENT-001");
    assert_contract(&verdict, "LIFE-003");
    for cascading in ["LIFE-004", "LIFE-005", "LIFE-006"] {
        assert!(
            !verdict
                .violations
                .iter()
                .any(|violation| violation.contract_id.as_str() == cascading)
        );
    }
}

#[test]
fn producer_contracts_detect_admission_terminal_and_certainty_failures() {
    let scenario = scenario(
        TerminalStatus::Acknowledged,
        VisibilityExpectation::ExactlyOnce,
    );
    let mut missing_admission = history(TerminalStatus::Acknowledged);
    remove_event(&mut missing_admission, |event| {
        matches!(event, AdapterEvent::OperationAccepted { .. })
    });
    assert_contract(
        &verify(
            &scenario,
            &adapter(),
            &missing_admission,
            &[observation(0, "value")],
        ),
        "PROD-001",
    );

    let mut missing_terminal = history(TerminalStatus::Acknowledged);
    remove_event(&mut missing_terminal, |event| {
        matches!(event, AdapterEvent::OperationTerminal { .. })
    });
    assert_contract(
        &verify(&scenario, &adapter(), &missing_terminal, &[]),
        "PROD-002",
    );

    assert_contract(
        &verify(
            &scenario,
            &adapter(),
            &history(TerminalStatus::PossiblySent),
            &[observation(0, "value")],
        ),
        "PROD-007",
    );
}

#[test]
fn producer_contracts_detect_visibility_contradictions() {
    let acknowledged = scenario(
        TerminalStatus::Acknowledged,
        VisibilityExpectation::ExactlyOnce,
    );
    assert_contract(
        &verify(
            &acknowledged,
            &adapter(),
            &history(TerminalStatus::Acknowledged),
            &[],
        ),
        "PROD-003",
    );

    let definitely_not_sent = scenario(
        TerminalStatus::DefinitelyNotSent,
        VisibilityExpectation::Absent,
    );
    assert_contract(
        &verify(
            &definitely_not_sent,
            &adapter(),
            &history(TerminalStatus::DefinitelyNotSent),
            &[observation(0, "value")],
        ),
        "PROD-004",
    );

    let expected_visible = scenario(
        TerminalStatus::PossiblySent,
        VisibilityExpectation::ExactlyOnce,
    );
    assert_contract(
        &verify(
            &expected_visible,
            &adapter(),
            &history(TerminalStatus::PossiblySent),
            &[],
        ),
        "PROD-008",
    );
}

#[test]
fn lifecycle_contracts_detect_every_missing_completion() {
    let scenario = scenario(
        TerminalStatus::Acknowledged,
        VisibilityExpectation::ExactlyOnce,
    );
    let cases: [(&str, EventPredicate); 7] = [
        ("LIFE-001", is_client_created),
        ("LIFE-007", is_client_ready),
        ("LIFE-002", is_producer_created),
        ("LIFE-003", is_flush_completed),
        ("LIFE-004", is_producer_closed),
        ("LIFE-005", is_client_shutdown),
        ("LIFE-006", is_finished),
    ];
    for (contract, predicate) in cases {
        let mut events = history(TerminalStatus::Acknowledged);
        remove_event(&mut events, predicate);
        let verdict = verify(&scenario, &adapter(), &events, &[observation(0, "value")]);
        assert_contract(&verdict, contract);
    }
}

fn remove_event(
    history: &mut Vec<testlab_schema::HistoryEntry>,
    predicate: impl Fn(&AdapterEvent) -> bool,
) {
    let position = history.iter().position(|entry| {
        let HistoryPayload::AdapterEvent { event } = &entry.payload else {
            return false;
        };
        predicate(&event.event)
    });
    if let Some(position) = position {
        history.remove(position);
    }
}

fn assert_contract(verdict: &Verdict, contract: &str) {
    assert!(
        verdict
            .violations
            .iter()
            .any(|violation| violation.contract_id.as_str() == contract),
        "missing contract {contract} in {:?}",
        verdict.violations
    );
}

fn operation_id(value: &str) -> OperationId {
    match OperationId::new(value) {
        Ok(value) => value,
        Err(error) => panic!("invalid fixture operation id: {error}"),
    }
}

fn client_id(value: &str) -> ClientId {
    match ClientId::new(value) {
        Ok(value) => value,
        Err(error) => panic!("invalid fixture client id: {error}"),
    }
}

fn producer_id(value: &str) -> ProducerId {
    match ProducerId::new(value) {
        Ok(value) => value,
        Err(error) => panic!("invalid fixture producer id: {error}"),
    }
}

fn is_client_created(event: &AdapterEvent) -> bool {
    matches!(event, AdapterEvent::ClientCreated { .. })
}

fn is_client_ready(event: &AdapterEvent) -> bool {
    matches!(event, AdapterEvent::ClientReady { .. })
}

fn is_producer_created(event: &AdapterEvent) -> bool {
    matches!(event, AdapterEvent::ProducerCreated { .. })
}

fn is_flush_completed(event: &AdapterEvent) -> bool {
    matches!(event, AdapterEvent::FlushCompleted { .. })
}

fn is_producer_closed(event: &AdapterEvent) -> bool {
    matches!(event, AdapterEvent::ProducerClosed { .. })
}

fn is_client_shutdown(event: &AdapterEvent) -> bool {
    matches!(event, AdapterEvent::ClientShutdown { .. })
}

fn is_finished(event: &AdapterEvent) -> bool {
    matches!(event, AdapterEvent::Finished)
}
