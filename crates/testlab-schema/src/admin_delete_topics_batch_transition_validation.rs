//! Plural deletion requires caller-selected topics to have explicit prior presence facts.

use std::collections::BTreeMap;

use crate::{Scenario, ScenarioAction, TopicSelection};

pub(crate) fn validate(scenario: &Scenario, problems: &mut Vec<String>) {
    let mut described = BTreeMap::<String, (bool, TopicSelection)>::new();
    for step in &scenario.steps {
        match &step.action {
            ScenarioAction::DescribeTopic(action) => {
                described.insert(
                    action.topic.clone(),
                    (action.expected_error_code.is_none(), TopicSelection::Name),
                );
            }
            ScenarioAction::DescribeTopics(action) => {
                for topic in &action.topics {
                    described.insert(
                        topic.topic.clone(),
                        (topic.expected_partitions.is_some(), action.selection),
                    );
                }
            }
            ScenarioAction::DeleteTopic(action) if action.expected_error_code.is_none() => {
                described.insert(action.topic.clone(), (false, TopicSelection::Name));
            }
            ScenarioAction::DeleteTopics(action) => {
                for topic in &action.topics {
                    let expected_present = topic.expected_error_code.is_none();
                    let expected = described.get(&topic.topic);
                    let selection_matches = action.selection == TopicSelection::Name
                        || expected
                            .is_some_and(|(_, selection)| *selection == TopicSelection::TopicId);
                    if expected.map(|(present, _)| *present) != Some(expected_present)
                        || !selection_matches
                    {
                        problems.push(format!(
                            "admin operation {} topic {} requires a matching prior topic description",
                            action.operation_id, topic.topic
                        ));
                    }
                    described.insert(topic.topic.clone(), (false, action.selection));
                }
            }
            _ => {}
        }
    }
}
