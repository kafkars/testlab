//! Aggregate group operation policy fails closed without both public durations.

use crate::{GroupOperationConfigMethod, Scenario, ScenarioAction};

#[test]
fn operation_config_requires_seek_and_close_durations() {
    let mut scenario: Scenario = toml::from_str(include_str!(
        "../../../scenarios/kafka/classic-group-seek-replay.toml"
    ))
    .unwrap_or_else(|error| panic!("parse aggregate operation scenario: {error}"));
    let configuration = scenario
        .steps
        .iter_mut()
        .find_map(|step| match &mut step.action {
            ScenarioAction::CreateGroupConsumer {
                configuration: Some(configuration),
                ..
            } => Some(configuration),
            _ => None,
        })
        .unwrap_or_else(|| panic!("configured group creation missing"));
    assert_eq!(
        configuration.operation_config_method,
        GroupOperationConfigMethod::OperationConfig
    );
    configuration.close_timeout_ms = None;
    let message = scenario
        .validation_error("aggregate operation config without close duration must fail")
        .to_string();
    assert!(
        message.contains("operation_config requires both"),
        "{message}"
    );
}
