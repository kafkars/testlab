//! Client metrics translation strips expectations while preserving both identities.

use testlab_schema::{AdapterCommand, Scenario, ScenarioAction};

use crate::runner_protocol::ExpectedEvent;

#[test]
fn metrics_translation_does_not_disclose_scenario_expectations() {
    let scenario: Scenario = toml::from_str(include_str!(
        "../../../scenarios/kafka/client-metrics-producer.toml"
    ))
    .unwrap_or_else(|error| panic!("parse client metrics: {error}"));
    let action = &scenario.steps[5].action;
    let ScenarioAction::ObserveClientMetrics(expected) = action else {
        panic!("client metrics action missing");
    };
    let Some((AdapterCommand::ObserveClientMetrics(command), event)) =
        crate::session_command::translate(action)
    else {
        panic!("client metrics translation missing");
    };
    assert_eq!(command.client_id, expected.client_id);
    assert_eq!(command.operation_id, expected.operation_id);
    assert!(matches!(
        event,
        ExpectedEvent::ClientMetricsObserved(client_id, operation_id)
            if client_id == expected.client_id && operation_id == expected.operation_id
    ));
}
