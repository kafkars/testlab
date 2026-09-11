//! Assigned-consumer client configuration crosses the wire without expectations.

use testlab_schema::{AdapterCommand, Scenario, ScenarioAction};

use crate::runner_protocol::ExpectedEvent;

#[test]
fn assigned_consumer_configuration_translates_without_policy_loss() {
    let scenario: Scenario = toml::from_str(include_str!(
        "../../../scenarios/kafka/assigned-consumer-read-committed.toml"
    ))
    .unwrap_or_else(|error| panic!("parse assigned-consumer configuration: {error}"));
    let action = &scenario.steps[0].action;
    let ScenarioAction::CreateAssignedConsumerClient(expected) = action else {
        panic!("configured assigned-consumer client action missing");
    };
    let Some((AdapterCommand::CreateAssignedConsumerClient(command), event)) =
        crate::session_command::translate(action)
    else {
        panic!("configured assigned-consumer client translation missing");
    };
    assert_eq!(&command, expected);
    assert!(matches!(
        event,
        ExpectedEvent::ClientCreated(client_id) if client_id == expected.client_id
    ));
}
