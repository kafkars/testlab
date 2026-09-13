//! Share command translation retains the selected public batch conversion.

use testlab_schema::{AdapterCommand, Scenario, ScenarioAction, ShareAcknowledgementMethod};

use crate::session_command_consumer::translate;

#[test]
fn accept_all_crosses_the_adapter_command_boundary() {
    let scenario: Scenario = toml::from_str(include_str!(
        "../../../scenarios/kafka/share-group-fetch-batch-size.toml"
    ))
    .unwrap_or_else(|error| panic!("parse accept-all scenario: {error}"));
    let step = scenario
        .steps
        .iter()
        .find(|step| step.id.as_str() == "acknowledge-three-acquisitions")
        .unwrap_or_else(|| panic!("accept-all acknowledgement missing"));
    let action = &step.action;

    let Some((AdapterCommand::ShareAcknowledge { method, .. }, _)) = translate(action) else {
        panic!("share acknowledgement must translate");
    };

    assert_eq!(method, ShareAcknowledgementMethod::AcceptAll);
    assert!(matches!(action, ScenarioAction::ShareAcknowledge { .. }));
}
