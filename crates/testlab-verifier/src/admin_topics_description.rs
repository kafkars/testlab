//! Plural topic descriptions join detailed caller-ordered outcomes to broker metadata.

use testlab_schema::{
    DescribeTopicExpectation, DescribeTopicsAction, ScenarioAction, TopicSelection, Violation,
};

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
                .all(|(actual, expected)| {
                    outcome_matches(
                        actual,
                        expected,
                        action.selection,
                        action.include_authorized_operations,
                    )
                })
    });
    let identities = index.topic_identities_observed.get(&action.operation_id);
    let independent_matches = match action.selection {
        TopicSelection::Name => public.is_some_and(|public| {
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
        }),
        TopicSelection::TopicId => public.is_some_and(|public| {
            identities.is_some_and(|values| {
                values.len() == action.topics.len()
                    && contiguous(values.iter().map(|value| value.observation))
                    && contiguous(values.iter().map(|value| value.history_sequence))
                    && values
                        .iter()
                        .zip(&action.topics)
                        .zip(&public.value.outcomes)
                        .all(|((actual, expected), outcome)| {
                            actual.topic == expected.topic
                                && actual.partitions.as_slice()
                                    == expected.expected_partitions.as_deref().unwrap_or_default()
                                && outcome.topic_id == Some(actual.topic_id)
                                && immediate_after_public(
                                    window,
                                    public.history_sequence,
                                    actual.history_sequence,
                                )
                        })
            })
        }),
    };
    if public_matches && independent_matches {
        return;
    }
    violations.push(violation(
        contract(action.selection),
        format!(
            "admin operation {} expected caller-ordered detailed {:?} topic outcomes with requested authorization metadata and immediate independent broker identity for every requested topic",
            action.operation_id, action.selection
        ),
        Some(action.operation_id.clone()),
        public
            .map(|value| format!("history:{}", value.history_sequence))
            .into_iter()
            .chain(independent.into_iter().flatten().map(|value| {
                format!("broker-state-observation:{}", value.observation)
            }))
            .chain(identities.into_iter().flatten().map(|value| {
                format!("broker-state-observation:{}", value.observation)
            }))
            .collect(),
    ));
}

fn outcome_matches(
    actual: &testlab_schema::AdminTopicDescriptionOutcome,
    expected: &DescribeTopicExpectation,
    selection: TopicSelection,
    include_authorized_operations: bool,
) -> bool {
    if actual.topic != expected.topic {
        return false;
    }
    let request_identity_matches = match selection {
        TopicSelection::Name => actual.topic_id.is_none(),
        TopicSelection::TopicId => actual.topic_id.is_some_and(|id| id != [0; 16]),
    };
    if !request_identity_matches {
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
                        && description.authorized_operations.is_some()
                            == include_authorized_operations
                        && actual
                            .topic_id
                            .is_none_or(|topic_id| description.topic_id == Some(topic_id))
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

const fn contract(selection: TopicSelection) -> &'static str {
    match selection {
        TopicSelection::Name => "ADMIN-044",
        TopicSelection::TopicId => "ADMIN-061",
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
