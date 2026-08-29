//! Producer cancellation translation retains its exact public command identity.

use testlab_schema::{AdapterCommand, Scenario, ScenarioAction};

use crate::runner_protocol::ExpectedEvent;

#[test]
fn cancellation_translates_without_losing_record_or_timeout() {
    let scenario: Scenario = toml::from_str(include_str!(
        "../../../scenarios/kafka/producer-cancellation.toml"
    ))
    .unwrap_or_else(|error| panic!("parse producer cancellation: {error}"));
    let action = &scenario.steps[3].action;
    let ScenarioAction::CancelProducerSend(expected) = action else {
        panic!("cancellation action missing");
    };
    let Some((AdapterCommand::CancelProducerSend(command), event)) =
        crate::session_command::translate(action)
    else {
        panic!("cancellation translation missing");
    };
    assert_eq!(&command, expected);
    assert!(matches!(
        event,
        ExpectedEvent::ProducerCancellationCompleted(operation_id)
            if operation_id == expected.operation_id
    ));
}
