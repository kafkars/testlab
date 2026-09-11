//! Share-group verification joins detailed public results to immediate Kafka CLI state.

use testlab_schema::{
    AdminShareGroupDescription, AdminShareGroupOffsetListing, BrokerShareGroupOffset, Scenario,
    ScenarioAction, Violation,
};

use crate::admin::{AdminCommandWindow, immediate_after_public, public_after_command};
use crate::index::HistoryIndex;
use crate::index::admin_share_group::Indexed;
use crate::support::violation;

pub(crate) fn verify_share_group_action(
    scenario: &Scenario,
    action: &ScenarioAction,
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) -> bool {
    match action {
        ScenarioAction::DescribeShareGroup(value) => {
            verify_description_action(action, value, index, violations)
        }
        ScenarioAction::DescribeShareGroups(value) => {
            crate::admin_share_groups_description::verify(
                scenario, action, value, index, violations,
            );
        }
        ScenarioAction::ListShareGroupOffsets(value) => {
            verify_offset_action(action, value, index, violations)
        }
        ScenarioAction::ListShareGroupsOffsets(value) => {
            crate::admin_share_groups_offsets::verify(action, value, index, violations);
        }
        ScenarioAction::AlterShareGroupOffsets(value) => {
            crate::admin_share_group_offset_mutation::verify(
                scenario, action, value, index, violations,
            );
        }
        ScenarioAction::DeleteShareGroupOffsets(value) => {
            crate::admin_share_group_offset_deletion::verify(
                scenario, action, value, index, violations,
            );
        }
        ScenarioAction::DeleteShareGroups(value) => {
            crate::admin_share_groups_deletion::verify(scenario, action, value, index, violations);
        }
        _ => return false,
    }
    true
}

fn verify_description_action(
    scenario_action: &ScenarioAction,
    action: &testlab_schema::DescribeShareGroupAction,
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) {
    let window = index.admin_command_window(scenario_action);
    let public = index.admin_share_groups.described.get(&action.operation_id);
    let independent = index.admin_share_groups.observed.get(&action.operation_id);
    let completion = one(public);
    let public_matches = completion.is_some_and(|value| {
        public_after_command(window, value.history_sequence)
            && description_matches(&value.value, action)
    });
    let independent_matches = completion.is_some_and(|value| {
        observation_matches(independent, action, window, value.history_sequence)
    });
    if !public_matches || !independent_matches {
        let evidence = public
            .into_iter()
            .flatten()
            .map(|value| format!("history:{}", value.history_sequence))
            .chain(
                independent
                    .into_iter()
                    .flatten()
                    .map(|value| format!("broker-state-observation:{}", value.value.observation)),
            )
            .collect();
        violations.push(violation(
            "ADMIN-037",
            format!(
                "admin operation {} expected exact public Share-group state and assignment plus immediate independent state",
                action.operation_id
            ),
            Some(action.operation_id.clone()),
            evidence,
        ));
    }
}

pub(crate) fn description_matches(
    actual: &AdminShareGroupDescription,
    expected: &testlab_schema::DescribeShareGroupAction,
) -> bool {
    actual.operation_id == expected.operation_id
        && actual.group_id == expected.group_id
        && actual.state == expected.expected_state
        && actual.group_epoch >= 0
        && actual.assignment_epoch >= 0
        && !actual.assignor_name.is_empty()
        && u32::try_from(actual.members.len()).ok() == Some(expected.expected_member_count)
        && valid_members(actual)
        && actual.members.iter().any(|member| {
            member
                .subscribed_topics
                .iter()
                .any(|topic| topic == &expected.expected_topic)
                && member.assignments.iter().any(|assignment| {
                    assignment.topic == expected.expected_topic
                        && assignment.partitions.contains(&expected.expected_partition)
                })
        })
}

fn valid_members(actual: &AdminShareGroupDescription) -> bool {
    actual
        .members
        .windows(2)
        .all(|pair| pair[0].member_id.as_bytes() < pair[1].member_id.as_bytes())
        && actual.members.iter().all(|member| {
            !member.member_id.is_empty()
                && member.member_epoch >= 0
                && !member.client_id.is_empty()
                && sorted_unique_nonempty(&member.subscribed_topics)
                && member.assignments.windows(2).all(|pair| {
                    pair[0].topic_id < pair[1].topic_id
                        || (pair[0].topic_id == pair[1].topic_id
                            && pair[0].topic.as_bytes() < pair[1].topic.as_bytes())
                })
                && member.assignments.iter().all(|assignment| {
                    assignment.topic_id != [0; 16]
                        && !assignment.topic.is_empty()
                        && !assignment.partitions.is_empty()
                        && assignment
                            .partitions
                            .iter()
                            .all(|partition| *partition >= 0)
                        && assignment
                            .partitions
                            .windows(2)
                            .all(|pair| pair[0] < pair[1])
                })
        })
}

fn sorted_unique_nonempty(values: &[String]) -> bool {
    !values.is_empty()
        && values
            .windows(2)
            .all(|pair| pair[0].as_bytes() < pair[1].as_bytes())
        && values.iter().all(|value| !value.is_empty())
}

fn observation_matches(
    actual: Option<&Vec<Indexed<testlab_schema::BrokerShareGroupState>>>,
    expected: &testlab_schema::DescribeShareGroupAction,
    window: Option<AdminCommandWindow>,
    public_sequence: u64,
) -> bool {
    let Some(actual) = actual else {
        return false;
    };
    let [actual] = actual.as_slice() else {
        return false;
    };
    actual.value.operation_id == expected.operation_id
        && actual.value.group_id == expected.group_id
        && actual.value.exists
        && actual.value.state.as_deref() == Some(expected.expected_state.as_str())
        && actual.value.member_count == Some(expected.expected_member_count)
        && immediate_after_public(window, public_sequence, actual.history_sequence)
}

fn verify_offset_action(
    scenario_action: &ScenarioAction,
    action: &testlab_schema::ListShareGroupOffsetsAction,
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) {
    let window = index.admin_command_window(scenario_action);
    let public = one(index
        .admin_share_groups
        .offsets_listed
        .get(&action.operation_id));
    let independent = one(index
        .admin_share_groups
        .offsets_observed
        .get(&action.operation_id));
    let public_matches = public.is_some_and(|value| {
        public_after_command(window, value.history_sequence) && offset_matches(&value.value, action)
    });
    let independent_matches = match (public, independent) {
        (Some(public), Some(independent)) => {
            offset_observation_matches(&independent.value, action)
                && immediate_after_public(
                    window,
                    public.history_sequence,
                    independent.history_sequence,
                )
        }
        _ => false,
    };
    if !public_matches || !independent_matches {
        let evidence = public
            .map(|value| format!("history:{}", value.history_sequence))
            .into_iter()
            .chain(
                independent
                    .map(|value| format!("broker-state-observation:{}", value.value.observation)),
            )
            .collect();
        violations.push(violation(
            "ADMIN-038",
            format!(
                "admin operation {} expected exact public and independent Share-group offset state",
                action.operation_id
            ),
            Some(action.operation_id.clone()),
            evidence,
        ));
    }
}

fn offset_matches(
    actual: &AdminShareGroupOffsetListing,
    expected: &testlab_schema::ListShareGroupOffsetsAction,
) -> bool {
    actual.operation_id == expected.operation_id
        && actual.group_id == expected.group_id
        && actual.topic == expected.topic
        && actual.partition == expected.partition
        && actual.topic_id != [0; 16]
        && actual.start_offset == Some(expected.expected_start_offset)
        && actual.leader_epoch.is_some_and(|epoch| epoch >= 0)
        && actual.lag == Some(expected.expected_lag)
        && actual.error_code.is_none()
}

fn offset_observation_matches(
    actual: &BrokerShareGroupOffset,
    expected: &testlab_schema::ListShareGroupOffsetsAction,
) -> bool {
    actual.operation_id == expected.operation_id
        && actual.group_id == expected.group_id
        && actual.topic == expected.topic
        && actual.partition == expected.partition
        && actual.start_offset == Some(expected.expected_start_offset)
        && actual.lag == Some(expected.expected_lag)
}

pub(crate) fn exact_offset_evidence(
    scenario_action: &ScenarioAction,
    action: &testlab_schema::ListShareGroupOffsetsAction,
    index: &HistoryIndex,
) -> bool {
    if index.admin_command_state(scenario_action) != (true, 1) {
        return false;
    }
    let window = index.admin_command_window(scenario_action);
    let public = one(index
        .admin_share_groups
        .offsets_listed
        .get(&action.operation_id));
    let independent = one(index
        .admin_share_groups
        .offsets_observed
        .get(&action.operation_id));
    match (public, independent) {
        (Some(public), Some(independent)) => {
            public_after_command(window, public.history_sequence)
                && offset_matches(&public.value, action)
                && offset_observation_matches(&independent.value, action)
                && immediate_after_public(
                    window,
                    public.history_sequence,
                    independent.history_sequence,
                )
        }
        _ => false,
    }
}

fn one<T>(values: Option<&Vec<Indexed<T>>>) -> Option<&Indexed<T>> {
    let [value] = values?.as_slice() else {
        return None;
    };
    Some(value)
}
