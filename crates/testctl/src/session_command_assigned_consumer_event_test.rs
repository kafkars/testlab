//! Direct-consumer event translation strips expected failure truth from adapter commands.

use testlab_schema::{AdapterCommand, AssignedConsumerEventMethod, Scenario, ScenarioAction};

use crate::runner_protocol::ExpectedEvent;

#[test]
fn event_translation_retains_only_public_request_identity() {
    let scenario: Scenario = toml::from_str(include_str!(
        "../../../scenarios/kafka/assigned-consumer-event-authorization-recovery.toml"
    ))
    .unwrap_or_else(|error| panic!("assigned event scenario: {error}"));
    let action = scenario
        .steps
        .iter()
        .find_map(|step| match &step.action {
            ScenarioAction::ObserveAssignedConsumerEvent(action)
                if action.method == AssignedConsumerEventMethod::TryTakeEvent =>
            {
                Some(action)
            }
            _ => None,
        })
        .unwrap_or_else(|| panic!("immediate event action missing"));

    let Some((AdapterCommand::ObserveAssignedConsumerEvent(command), expected)) =
        crate::session_command_consumer::translate(&ScenarioAction::ObserveAssignedConsumerEvent(
            action.clone(),
        ))
    else {
        panic!("assigned event action must translate");
    };
    assert_eq!(command.operation_id, action.operation_id);
    assert_eq!(command.consumer_id, action.consumer_id);
    assert_eq!(command.method, AssignedConsumerEventMethod::TryTakeEvent);
    assert_eq!(command.timeout_ms, action.timeout_ms);
    assert!(matches!(
        expected,
        ExpectedEvent::AssignedConsumerEventObserved(operation_id)
            if operation_id == action.operation_id
    ));
}
