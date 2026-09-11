//! Plural deletion requires caller-selected topics to have explicit prior presence facts.

use std::collections::BTreeMap;

use crate::{Scenario, ScenarioAction};

pub(crate) fn validate(scenario: &Scenario, problems: &mut Vec<String>) {
    let mut described = BTreeMap::<String, bool>::new();
    for step in &scenario.steps {
        match &step.action {
            ScenarioAction::DescribeTopic(action) => {
                described.insert(action.topic.clone(), action.expected_error_code.is_none());
            }
            ScenarioAction::DescribeTopics(action) => {
                for topic in &action.topics {
                    described.insert(topic.topic.clone(), topic.expected_partitions.is_some());
                }
            }
            ScenarioAction::DeleteTopic(action) if action.expected_error_code.is_none() => {
                described.insert(action.topic.clone(), false);
            }
            ScenarioAction::DeleteTopics(action) => {
                for topic in &action.topics {
                    let expected_present = topic.expected_error_code.is_none();
                    if described.get(&topic.topic) != Some(&expected_present) {
                        problems.push(format!(
                            "admin operation {} topic {} requires a matching prior topic description",
                            action.operation_id, topic.topic
                        ));
                    }
                    described.insert(topic.topic.clone(), false);
                }
            }
            _ => {}
        }
    }
}
