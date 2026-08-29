//! Topic-configuration mutations require an independently checkable prior description.

use std::collections::BTreeMap;

use crate::{Scenario, ScenarioAction};

pub(crate) fn validate(scenario: &Scenario, problems: &mut Vec<String>) {
    let mut described = BTreeMap::<(String, String), String>::new();
    for step in &scenario.steps {
        match &step.action {
            ScenarioAction::DescribeTopicConfig(action) => {
                described.insert(
                    (action.topic.clone(), action.config_name.clone()),
                    action.expected_value.clone(),
                );
            }
            ScenarioAction::AlterTopicConfig(action) => {
                let key = (action.topic.clone(), action.config_name.clone());
                if action.validate_only {
                    crate::admin_validate_only_validation::validate_config_transition(
                        action,
                        described.get(&key).map(String::as_str),
                        problems,
                    );
                } else {
                    match described.remove(&key) {
                        Some(value) if value != action.value => {}
                        Some(_) => problems.push(format!(
                            "admin operation {} requires a prior different topic-configuration value",
                            action.operation_id
                        )),
                        None => problems.push(format!(
                            "admin operation {} requires a prior topic-configuration description",
                            action.operation_id
                        )),
                    }
                }
            }
            _ => {}
        }
    }
}
