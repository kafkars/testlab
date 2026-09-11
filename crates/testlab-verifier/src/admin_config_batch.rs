//! Plural topic-configuration descriptions join ordered public outcomes to broker reads.

use testlab_schema::{DescribeTopicConfigsAction, ScenarioAction, Violation};

use crate::admin::{immediate_after_public, public_after_command};
use crate::index::{HistoryIndex, IndexedAdminTopicConfigsDescription};
use crate::support::violation;

pub(crate) fn verify_config_batch_action(
    scenario_action: &ScenarioAction,
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) -> bool {
    let ScenarioAction::DescribeTopicConfigs(action) = scenario_action else {
        return false;
    };
    verify(scenario_action, action, index, violations);
    true
}

fn verify(
    scenario_action: &ScenarioAction,
    action: &DescribeTopicConfigsAction,
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) {
    let window = index.admin_command_window(scenario_action);
    let public = one(index
        .topic_configs_batch_described
        .get(&action.operation_id));
    let independent = index.topic_configs_observed.get(&action.operation_id);
    let public_matches = public.is_some_and(|indexed| {
        public_after_command(window, indexed.history_sequence)
            && indexed.value.operation_id == action.operation_id
            && indexed.value.outcomes.len() == action.topics.len()
            && indexed
                .value
                .outcomes
                .iter()
                .zip(&action.topics)
                .all(|(actual, expected)| {
                    actual.topic == expected.topic
                        && actual.config_name == expected.config_name
                        && actual.value.as_deref() == Some(expected.expected_value.as_str())
                        && actual.error_code.is_none()
                })
    });
    let independent_matches = public.is_some_and(|public| {
        independent.is_some_and(|values| {
            values.len() == action.topics.len()
                && contiguous(values.iter().map(|value| value.observation))
                && contiguous(values.iter().map(|value| value.history_sequence))
                && values.iter().zip(&action.topics).all(|(actual, expected)| {
                    actual.topic == expected.topic
                        && actual.config_name == expected.config_name
                        && actual.value == expected.expected_value
                        && immediate_after_public(
                            window,
                            public.history_sequence,
                            actual.history_sequence,
                        )
                })
        })
    });
    if public_matches && independent_matches {
        return;
    }
    violations.push(violation(
        "ADMIN-048",
        format!(
            "admin operation {} expected caller-ordered selected configuration values and immediate independent reads for every requested topic",
            action.operation_id
        ),
        Some(action.operation_id.clone()),
        public
            .map(|value| format!("history:{}", value.history_sequence))
            .into_iter()
            .chain(independent.into_iter().flatten().map(|value| {
                format!("broker-state-observation:{}", value.observation)
            }))
            .collect(),
    ));
}

fn contiguous(values: impl Iterator<Item = u64>) -> bool {
    let mut previous: Option<u64> = None;
    for value in values {
        if previous.is_some_and(|previous| value != previous.saturating_add(1)) {
            return false;
        }
        previous = Some(value);
    }
    true
}

fn one(
    values: Option<&Vec<IndexedAdminTopicConfigsDescription>>,
) -> Option<&IndexedAdminTopicConfigsDescription> {
    let [value] = values?.as_slice() else {
        return None;
    };
    Some(value)
}
