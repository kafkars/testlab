//! Policy command tests keep scenario expectations outside the adapter protocol.

use testlab_schema::{
    AdapterCommand, ClientId, ConsumerId, OperationId, ProducerId, ScenarioAction,
};

#[test]
fn group_error_expectation_is_not_sent_to_the_adapter() {
    let action = ScenarioAction::GroupReceive {
        consumer_id: consumer("consumer-1"),
        receive_id: operation("receive-1"),
        expected_operation_id: operation("op-1"),
        expected_error_code: Some("broker:broker_30".to_owned()),
        timeout_ms: 1_000,
    };

    let Some((command, _)) = super::session_command::translate(&action) else {
        panic!("group receive must translate");
    };

    assert!(matches!(
        command,
        AdapterCommand::GroupReceive {
            timeout_ms: 1_000,
            ..
        }
    ));
}

#[test]
fn transaction_error_expectation_is_not_sent_to_the_adapter() {
    let action = ScenarioAction::CreateTransactionalProducer {
        client_id: client("client-1"),
        producer_id: producer("producer-1"),
        transactional_id: "transactional-1".to_owned(),
        transaction_timeout_ms: 1_000,
        initialization_timeout_ms: 2_000,
        expected_error_code: Some("broker:broker_53".to_owned()),
    };

    let Some((command, _)) = super::session_command::translate(&action) else {
        panic!("transaction initialization must translate");
    };

    assert!(matches!(
        command,
        AdapterCommand::CreateTransactionalProducer {
            transaction_timeout_ms: 1_000,
            initialization_timeout_ms: 2_000,
            ..
        }
    ));
}

fn client(value: &str) -> ClientId {
    ClientId::new(value).unwrap_or_else(|error| panic!("client id: {error}"))
}

fn consumer(value: &str) -> ConsumerId {
    ConsumerId::new(value).unwrap_or_else(|error| panic!("consumer id: {error}"))
}

fn operation(value: &str) -> OperationId {
    OperationId::new(value).unwrap_or_else(|error| panic!("operation id: {error}"))
}

fn producer(value: &str) -> ProducerId {
    ProducerId::new(value).unwrap_or_else(|error| panic!("producer id: {error}"))
}
