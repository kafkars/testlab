//! Active-producer protocol tests pin expectation-free exact target identity.

use testlab_schema::{
    AdapterCommand, AdapterEvent, AdminProducersDescription, ClientId, DescribeProducersAction,
    OperationId, ScenarioAction,
};

use crate::runner_protocol::{EventDisposition, ExpectedEvent};

#[test]
fn translation_keeps_expected_count_off_wire() {
    let action = ScenarioAction::DescribeProducers(action());
    let Some((AdapterCommand::DescribeProducers(command), expected)) =
        crate::session_command_admin::translate(&action)
    else {
        panic!("active-producer translation");
    };
    assert_eq!(command.client_id, client());
    assert_eq!(command.operation_id, operation());
    assert_eq!(command.topic, "orders");
    assert_eq!(command.partition, 2);
    assert_eq!(command.broker_id, Some(7));
    assert_eq!(command.timeout_ms, 1_000);
    let encoded = serde_json::to_string(&command)
        .unwrap_or_else(|error| panic!("encode active-producer command: {error}"));
    assert!(!encoded.contains("expected_producer_count"));
    assert!(matches!(
        expected,
        ExpectedEvent::ProducerStatesDescribed { .. }
    ));
}

#[test]
fn completion_requires_exact_operation_topic_and_partition() {
    let expected = ExpectedEvent::ProducerStatesDescribed {
        operation_id: operation(),
        topic: "orders".to_owned(),
        partition: 2,
    };
    let mut event = AdapterEvent::ProducersDescribed(description());
    assert_eq!(
        expected
            .classify(&event)
            .unwrap_or_else(|error| panic!("classify active-producer event: {error}")),
        EventDisposition::Complete
    );
    let AdapterEvent::ProducersDescribed(actual) = &mut event else {
        panic!("active-producer event");
    };
    actual.partition = 3;
    let error = expected
        .classify(&event)
        .err()
        .unwrap_or_else(|| panic!("foreign partition must not complete"));
    assert_eq!(error.harness_error().code, "event_identity_mismatch");
}

fn action() -> DescribeProducersAction {
    DescribeProducersAction {
        client_id: client(),
        operation_id: operation(),
        topic: "orders".to_owned(),
        partition: 2,
        broker_id: Some(7),
        expected_producer_count: 1,
        timeout_ms: 1_000,
    }
}

fn description() -> AdminProducersDescription {
    AdminProducersDescription {
        operation_id: operation(),
        topic: "orders".to_owned(),
        partition: 2,
        producers: Vec::new(),
    }
}

fn client() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client: {error}"))
}

fn operation() -> OperationId {
    OperationId::new("describe-producers").unwrap_or_else(|error| panic!("operation: {error}"))
}
