//! Streams-group verification covers every public Admin method as one lifecycle.

use testlab_schema::{
    AdminStreamsGroupAdminLifecycle, AdminStreamsGroupDescription, AdminStreamsGroupOffset,
    ExerciseStreamsGroupAdminLifecycleAction, ScenarioAction, Violation,
};

use crate::admin::{immediate_after_public, public_after_command};
use crate::index::HistoryIndex;
use crate::index::admin_lifecycle::Indexed;
use crate::support::violation;

const MAX_METADATA_BYTES: usize = 8 * 1024;

pub(crate) fn verify(
    scenario_action: &ScenarioAction,
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) -> bool {
    let ScenarioAction::ExerciseStreamsGroupAdminLifecycle(action) = scenario_action else {
        return false;
    };
    let window = index.admin_command_window(scenario_action);
    let public = one(index
        .admin_lifecycles
        .streams_completed
        .get(&action.operation_id));
    let independent = one(index
        .admin_lifecycles
        .streams_observed
        .get(&action.operation_id));
    let matches = public.is_some_and(|public| {
        independent.is_some_and(|independent| {
            public_matches(&public.value, action)
                && public_after_command(window, public.history_sequence)
                && independent.value.operation_id == action.operation_id
                && independent.value.group_ids
                    == [
                        action.secondary_group_id.clone(),
                        action.primary_group_id.clone(),
                    ]
                && independent.value.all_absent
                && immediate_after_public(
                    window,
                    public.history_sequence,
                    independent.history_sequence,
                )
        })
    });
    if !matches {
        violations.push(violation(
            "ADMIN-074",
            format!(
                "admin operation {} expected exact caller-ordered Streams-group descriptions, stable offsets, mutation, offset deletion, group deletion, and immediate independent final absence",
                action.operation_id
            ),
            Some(action.operation_id.clone()),
            public
                .map(|value| format!("history:{}", value.history_sequence))
                .into_iter()
                .chain(independent.map(|value| {
                    format!("broker-state-observation:{}", value.value.observation)
                }))
                .collect(),
        ));
    }
    true
}

fn public_matches(
    actual: &AdminStreamsGroupAdminLifecycle,
    expected: &ExerciseStreamsGroupAdminLifecycleAction,
) -> bool {
    let groups = [
        expected.secondary_group_id.as_str(),
        expected.primary_group_id.as_str(),
    ];
    actual.operation_id == expected.operation_id
        && description_matches(
            &actual.singular_description,
            &expected.primary_group_id,
            expected,
        )
        && exact_descriptions(&actual.plural_descriptions, &groups, expected)
        && offset_matches(
            &actual.singular_initial_offset,
            &expected.primary_group_id,
            Some(expected.expected_initial_offset),
            expected,
        )
        && exact_offsets(
            &actual.plural_initial_offsets,
            &groups,
            Some(expected.expected_initial_offset),
            expected,
        )
        && offset_matches(
            &actual.offset_after_alter,
            &expected.primary_group_id,
            Some(expected.altered_offset),
            expected,
        )
        && actual.deleted_offset.group_id == expected.primary_group_id
        && actual.deleted_offset.topic == expected.input_topic
        && actual.deleted_offset.partition == 0
        && offset_matches(
            &actual.offset_after_delete,
            &expected.primary_group_id,
            None,
            expected,
        )
        && actual.deleted_group_ids == groups
        && actual
            .throttle_times_ms
            .iter()
            .all(|throttle| *throttle <= expected.timeout_ms)
}

fn exact_descriptions(
    actual: &[AdminStreamsGroupDescription],
    groups: &[&str],
    expected: &ExerciseStreamsGroupAdminLifecycleAction,
) -> bool {
    actual.len() == groups.len()
        && actual
            .iter()
            .zip(groups)
            .all(|(actual, group)| description_matches(actual, group, expected))
}

fn description_matches(
    actual: &AdminStreamsGroupDescription,
    group: &str,
    expected: &ExerciseStreamsGroupAdminLifecycleAction,
) -> bool {
    actual.group_id == group
        && actual.state == "Empty"
        && actual.group_epoch >= 0
        && actual.assignment_epoch >= 0
        && actual.topology_epoch.is_some_and(|epoch| epoch >= 0)
        && actual.member_count == 0
        && actual.authorized_operations.is_some()
        && valid_topology_description(actual)
        && actual
            .topology_subtopology_count
            .is_some_and(|count| count > 0)
        && sorted_unique_nonempty(&actual.topology_source_topics)
        && actual
            .topology_source_topics
            .iter()
            .any(|topic| topic == &expected.input_topic)
}

fn valid_topology_description(actual: &AdminStreamsGroupDescription) -> bool {
    match actual.topology_description_status {
        Some(3) => actual
            .topology_description_subtopology_count
            .is_some_and(|count| count > 0),
        Some(1) => actual.topology_description_subtopology_count.is_none(),
        _ => false,
    }
}

fn exact_offsets(
    actual: &[AdminStreamsGroupOffset],
    groups: &[&str],
    offset: Option<i64>,
    expected: &ExerciseStreamsGroupAdminLifecycleAction,
) -> bool {
    actual.len() == groups.len()
        && actual
            .iter()
            .zip(groups)
            .all(|(actual, group)| offset_matches(actual, group, offset, expected))
}

fn offset_matches(
    actual: &AdminStreamsGroupOffset,
    group: &str,
    offset: Option<i64>,
    expected: &ExerciseStreamsGroupAdminLifecycleAction,
) -> bool {
    actual.group_id == group
        && actual.topic == expected.input_topic
        && actual.partition == 0
        && actual.committed_offset == offset
        && actual.leader_epoch.is_none_or(|epoch| epoch >= 0)
        && actual.metadata.as_ref().is_none_or(|metadata| {
            metadata.len() <= MAX_METADATA_BYTES && !metadata.chars().any(char::is_control)
        })
}

fn sorted_unique_nonempty(values: &[String]) -> bool {
    !values.is_empty()
        && values
            .windows(2)
            .all(|pair| pair[0].as_bytes() < pair[1].as_bytes())
        && values
            .iter()
            .all(|value| !value.is_empty() && !value.chars().any(char::is_control))
}

fn one<T>(values: Option<&Vec<Indexed<T>>>) -> Option<&Indexed<T>> {
    let [value] = values?.as_slice() else {
        return None;
    };
    Some(value)
}

#[cfg(test)]
#[path = "admin_streams_group_test.rs"]
mod tests;
