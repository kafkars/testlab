//! Plural Share offsets join ordered public results to immediate Kafka CLI facts.

use testlab_schema::{ListShareGroupsOffsetsAction, ScenarioAction, Violation};

use crate::admin::{immediate_after_public, public_after_command};
use crate::index::HistoryIndex;
use crate::index::admin_share_group::Indexed;
use crate::support::violation;

pub(crate) fn verify(
    scenario_action: &ScenarioAction,
    action: &ListShareGroupsOffsetsAction,
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) {
    let window = index.admin_command_window(scenario_action);
    let public = one(index
        .admin_share_groups
        .groups_offsets_listed
        .get(&action.operation_id));
    let independent = index
        .admin_share_groups
        .offsets_observed
        .get(&action.operation_id);
    let public_matches = public.is_some_and(|value| {
        public_after_command(window, value.history_sequence)
            && value.value.operation_id == action.operation_id
            && value.value.groups.len() == action.groups.len()
            && value
                .value
                .groups
                .iter()
                .zip(&action.groups)
                .all(|(actual, expected)| {
                    actual.group_id == expected.group_id
                        && actual.error_code.is_none()
                        && exact_public_offsets(&actual.offsets, &expected.partitions)
                })
    });
    let independent_matches = public.is_some_and(|public| {
        exact_independent_offsets(action, independent, window, public.history_sequence)
    });
    if public_matches && independent_matches {
        return;
    }
    violations.push(violation(
        "ADMIN-043",
        format!(
            "admin operation {} expected caller-ordered public and independent Share-group offset state for every selected partition",
            action.operation_id
        ),
        Some(action.operation_id.clone()),
        public
            .map(|value| format!("history:{}", value.history_sequence))
            .into_iter()
            .chain(independent.into_iter().flatten().map(|value| {
                format!("broker-state-observation:{}", value.value.observation)
            }))
            .collect(),
    ));
}

fn exact_public_offsets(
    actual: &[testlab_schema::AdminShareGroupOffsetOutcome],
    expected: &[testlab_schema::ShareGroupOffsetExpectation],
) -> bool {
    actual.len() == expected.len()
        && actual.iter().zip(expected).all(|(actual, expected)| {
            actual.topic == expected.topic
                && actual.partition == expected.partition
                && actual.topic_id != [0; 16]
                && actual.start_offset == Some(expected.expected_start_offset)
                && actual.leader_epoch.is_some_and(|epoch| epoch >= 0)
                && actual.lag == Some(expected.expected_lag)
                && actual.error_code.is_none()
        })
}

fn exact_independent_offsets(
    action: &ListShareGroupsOffsetsAction,
    actual: Option<&Vec<Indexed<testlab_schema::BrokerShareGroupOffset>>>,
    window: Option<crate::admin::AdminCommandWindow>,
    public_sequence: u64,
) -> bool {
    let flattened = action.groups.iter().flat_map(|group| {
        group
            .partitions
            .iter()
            .map(move |partition| (group.group_id.as_str(), partition))
    });
    actual.is_some_and(|actual| {
        let count = action
            .groups
            .iter()
            .map(|group| group.partitions.len())
            .sum::<usize>();
        actual.len() == count
            && contiguous(actual.iter().map(|value| value.value.observation))
            && contiguous(actual.iter().map(|value| value.history_sequence))
            && actual
                .iter()
                .zip(flattened)
                .all(|(actual, (group_id, expected))| {
                    actual.value.operation_id == action.operation_id
                        && actual.value.group_id == group_id
                        && actual.value.topic == expected.topic
                        && actual.value.partition == expected.partition
                        && actual.value.start_offset == Some(expected.expected_start_offset)
                        && actual.value.lag == Some(expected.expected_lag)
                        && immediate_after_public(window, public_sequence, actual.history_sequence)
                })
    })
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

fn one<T>(values: Option<&Vec<Indexed<T>>>) -> Option<&Indexed<T>> {
    let [value] = values?.as_slice() else {
        return None;
    };
    Some(value)
}
