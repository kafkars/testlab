//! Validation-only feature updates require a prior independent feature snapshot.

use std::collections::BTreeSet;

use crate::{OperationId, Scenario, ScenarioAction};

pub(crate) fn validate(scenario: &Scenario, problems: &mut Vec<String>) {
    let mut descriptions = BTreeSet::<OperationId>::new();
    for step in &scenario.steps {
        match &step.action {
            ScenarioAction::DescribeFeatures(action) => {
                descriptions.insert(action.operation_id.clone());
            }
            ScenarioAction::ValidateFeatureUpdates(action)
                if !descriptions.contains(&action.baseline_operation_id) =>
            {
                problems.push(format!(
                    "admin operation {} requires prior feature baseline {}",
                    action.operation_id, action.baseline_operation_id
                ));
            }
            _ => {}
        }
    }
}
