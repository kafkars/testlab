//! Plural topic descriptions join detailed caller-ordered outcomes to broker metadata.

use testlab_schema::{DescribeTopicExpectation, DescribeTopicsAction, ScenarioAction, Violation};

use crate::admin::{immediate_after_public, public_after_command};
use crate::index::{HistoryIndex, IndexedAdminTopicsDescription};
use crate::support::violation;

pub(crate) fn verify_topics_description_action(
    scenario_action: &ScenarioAction,
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) -> bool {
    let ScenarioAction::DescribeTopics(action) = scenario_action else {
        return false;
    };
    verify(scenario_action, action, index, violations);
    true
}

fn verify(
    scenario_action: &ScenarioAction,
    action: &DescribeTopicsAction,
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) {
    let window = index.admin_command_window(scenario_action);
    let public = one(index.topics_batch_described.get(&action.operation_id));
    let independent = index.topics_observed.get(&action.operation_id);
    let public_matches = public.is_some_and(|indexed| {
        public_after_command(window, indexed.history_sequence)
            && indexed.value.operation_id == action.operation_id
            && indexed.value.outcomes.len() == action.topics.len()
            && indexed
                .value
                .outcomes
                .iter()
                .zip(&action.topics)
                .all(|(actual, expected)| outcome_matches(actual, expected))
    });
    let independent_matches = public.is_some_and(|public| {
        independent.is_some_and(|values| {
            values.len() == action.topics.len()
                && contiguous(values.iter().map(|value| value.observation))
                && contiguous(values.iter().map(|value| value.history_sequence))
                && values.iter().zip(&action.topics).all(|(actual, expected)| {
                    actual.topic == expected.topic
                        && state_matches(actual.exists, &actual.partitions, expected)
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
        "ADMIN-044",
        format!(
            "admin operation {} expected caller-ordered detailed topic outcomes and immediate independent metadata for every requested topic",
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

fn outcome_matches(
    actual: &testlab_schema::AdminTopicDescriptionOutcome,
    expected: &DescribeTopicExpectation,
) -> bool {
    if actual.topic != expected.topic {
        return false;
    }
    match (
        expected.expected_partitions.as_deref(),
        expected.expected_error_code.as_deref(),
    ) {
        (Some(partitions), None) => {
            actual.error_code.is_none()
                && actual.description.as_ref().is_some_and(|description| {
                    description.topic_id.is_some_and(|id| id != [0; 16])
                        && !description.internal
                        && description.partitions.len() == partitions.len()
                        && description.partitions.iter().zip(partitions).all(
                            |(actual, expected)| {
                                actual.partition == *expected && actual.error_code.is_none()
                            },
                        )
                })
        }
        (None, Some(error)) => {
            actual.description.is_none() && actual.error_code.as_deref() == Some(error)
        }
        _ => false,
    }
}

fn state_matches(exists: bool, partitions: &[i32], expected: &DescribeTopicExpectation) -> bool {
    match expected.expected_partitions.as_deref() {
        Some(expected) => exists && partitions == expected,
        None => !exists && partitions.is_empty(),
    }
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
    values: Option<&Vec<IndexedAdminTopicsDescription>>,
) -> Option<&IndexedAdminTopicsDescription> {
    let [value] = values?.as_slice() else {
        return None;
    };
    Some(value)
}
