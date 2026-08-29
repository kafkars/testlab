//! Producer configuration translation retains complete client-wide public policy.

use testlab_schema::{AdapterCommand, Scenario, ScenarioAction};

use crate::runner_protocol::ExpectedEvent;

#[test]
fn configured_client_translates_without_policy_loss() {
    let scenario: Scenario = toml::from_str(include_str!(
        "../../../scenarios/kafka/producer-configuration-gzip.toml"
    ))
    .unwrap_or_else(|error| panic!("parse producer configuration: {error}"));
    let action = &scenario.steps[0].action;
    let ScenarioAction::CreateConfiguredClient(expected) = action else {
        panic!("configured client action missing");
    };
    let Some((AdapterCommand::CreateConfiguredClient(command), event)) =
        crate::session_command::translate(action)
    else {
        panic!("configured client translation missing");
    };
    assert_eq!(&command, expected);
    assert!(matches!(
        event,
        ExpectedEvent::ClientCreated(client_id) if client_id == expected.client_id
    ));
}
