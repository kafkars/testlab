//! Tests for the admin config transition validation contract.

use crate::{Scenario, ScenarioAction};

#[test]
fn configured_creation_requires_its_ordered_description() {
    let scenario = fixture();
    let mut problems = Vec::new();
    super::validate(&scenario, &mut problems);
    assert!(
        !problems
            .iter()
            .any(|problem| problem.contains("configured topic creation requires")),
        "{problems:?}"
    );

    let mut missing = scenario;
    missing.steps.retain(|step| {
        !matches!(
            &step.action,
            ScenarioAction::DescribeTopicConfig(action)
                if action.operation_id.as_str() == "admin-create-config-described"
        )
    });
    let mut problems = Vec::new();
    super::validate(&missing, &mut problems);
    assert!(
        problems
            .iter()
            .any(|problem| problem.contains("configured topic creation requires")),
        "{problems:?}"
    );
}

fn fixture() -> Scenario {
    toml::from_str(include_str!(
        "../../../scenarios/kafka/admin-create-topic.toml"
    ))
    .unwrap_or_else(|error| panic!("parse configured topic creation: {error}"))
}
