//! Topic-configuration mutations require an independently checkable prior description.

use std::collections::BTreeMap;

use crate::{DescribeTopicConfigExpectation, OperationId, Scenario, ScenarioAction};

type ConfigKey = (String, String);

pub(crate) fn validate(scenario: &Scenario, problems: &mut Vec<String>) {
    let mut described = BTreeMap::<ConfigKey, String>::new();
    let mut described_batches = BTreeMap::<OperationId, Vec<DescribeTopicConfigExpectation>>::new();
    for step in &scenario.steps {
        match &step.action {
            ScenarioAction::DescribeTopicConfig(action) => {
                described.insert(
                    (action.topic.clone(), action.config_name.clone()),
                    action.expected_value.clone(),
                );
            }
            ScenarioAction::DescribeTopicConfigs(action) => {
                described_batches.insert(action.operation_id.clone(), action.topics.clone());
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
                    invalidate_batches(&mut described_batches, std::slice::from_ref(&key));
                }
            }
            ScenarioAction::AlterTopicConfigs(action) => {
                validate_batch(action, &described_batches, problems);
                let keys = action
                    .topics
                    .iter()
                    .map(|selected| (selected.topic.clone(), selected.config_name.clone()))
                    .collect::<Vec<_>>();
                for key in &keys {
                    described.remove(key);
                }
                invalidate_batches(&mut described_batches, &keys);
            }
            _ => {}
        }
    }
}

fn validate_batch(
    action: &crate::AlterTopicConfigsAction,
    described: &BTreeMap<OperationId, Vec<DescribeTopicConfigExpectation>>,
    problems: &mut Vec<String>,
) {
    let Some(baseline) = described.get(&action.baseline_operation_id) else {
        problems.push(format!(
            "admin operation {} requires prior plural topic-configuration baseline {}",
            action.operation_id, action.baseline_operation_id
        ));
        return;
    };
    if baseline.len() != action.topics.len()
        || !baseline.iter().zip(&action.topics).all(|(before, after)| {
            before.topic == after.topic
                && before.config_name == after.config_name
                && before.expected_value == after.expected_previous_value
        })
    {
        problems.push(format!(
            "admin operation {} does not exactly match plural configuration baseline {}",
            action.operation_id, action.baseline_operation_id
        ));
    }
    for selected in &action.topics {
        if selected.expected_previous_value == selected.value {
            problems.push(format!(
                "admin operation {} requires a different prior value for {}:{}",
                action.operation_id, selected.topic, selected.config_name
            ));
        }
    }
}

fn invalidate_batches(
    described: &mut BTreeMap<OperationId, Vec<DescribeTopicConfigExpectation>>,
    changed: &[ConfigKey],
) {
    described.retain(|_, batch| {
        !batch.iter().any(|selected| {
            changed.iter().any(|(topic, config_name)| {
                selected.topic.as_str() == topic.as_str()
                    && selected.config_name.as_str() == config_name.as_str()
            })
        })
    });
}
