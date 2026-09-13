//! Topic-configuration mutations require an independently checkable prior description.

use std::collections::BTreeMap;

use crate::{
    DescribeTopicConfigExpectation, OperationId, Scenario, ScenarioAction, TopicConfigApi,
};

type ConfigKey = (String, String);

pub(crate) fn validate(scenario: &Scenario, problems: &mut Vec<String>) {
    validate_creation_descriptions(scenario, problems);
    let mut described = BTreeMap::<ConfigKey, String>::new();
    let mut described_batches =
        BTreeMap::<OperationId, (TopicConfigApi, Vec<DescribeTopicConfigExpectation>)>::new();
    for step in &scenario.steps {
        match &step.action {
            ScenarioAction::DescribeTopicConfig(action) => {
                described.insert(
                    (action.topic.clone(), action.config_name.clone()),
                    action.expected_value.clone(),
                );
            }
            ScenarioAction::DescribeTopicConfigs(action) => {
                described_batches.insert(
                    action.operation_id.clone(),
                    (action.api, action.topics.clone()),
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

fn validate_creation_descriptions(scenario: &Scenario, problems: &mut Vec<String>) {
    for (step_index, step) in scenario.steps.iter().enumerate() {
        let ScenarioAction::CreateTopic(creation) = &step.action else {
            continue;
        };
        if creation.expected_error_code.is_some()
            || creation.validate_only
            || creation.replica_assignments.is_some()
            || creation.configs.is_empty()
        {
            continue;
        }
        let mut following = scenario.steps.iter().skip(step_index.saturating_add(1));
        for config in &creation.configs {
            let described = following.by_ref().any(|step| {
                matches!(
                    &step.action,
                    ScenarioAction::DescribeTopicConfig(action)
                        if action.topic == creation.topic
                            && action.config_name == config.name
                            && action.expected_value == config.value
                )
            });
            if !described {
                problems.push(format!(
                    "admin operation {} configured topic creation requires a later ordered description of {}:{}",
                    creation.operation_id, creation.topic, config.name
                ));
                break;
            }
        }
    }
}

fn validate_batch(
    action: &crate::AlterTopicConfigsAction,
    described: &BTreeMap<OperationId, (TopicConfigApi, Vec<DescribeTopicConfigExpectation>)>,
    problems: &mut Vec<String>,
) {
    let Some((baseline_api, baseline)) = described.get(&action.baseline_operation_id) else {
        problems.push(format!(
            "admin operation {} requires prior plural topic-configuration baseline {}",
            action.operation_id, action.baseline_operation_id
        ));
        return;
    };
    if *baseline_api != action.api.description_api()
        || baseline.len() != action.topics.len()
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
    described: &mut BTreeMap<OperationId, (TopicConfigApi, Vec<DescribeTopicConfigExpectation>)>,
    changed: &[ConfigKey],
) {
    described.retain(|_, (_, batch)| {
        !batch.iter().any(|selected| {
            changed.iter().any(|(topic, config_name)| {
                selected.topic.as_str() == topic.as_str()
                    && selected.config_name.as_str() == config_name.as_str()
            })
        })
    });
}

#[cfg(test)]
mod creation_tests {
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
}
