//! Share-group verification joins detailed public results to immediate Kafka CLI state.

use testlab_schema::{AdminShareGroupDescription, ScenarioAction, Violation};

use crate::admin::{AdminCommandWindow, immediate_after_public, public_after_command};
use crate::index::HistoryIndex;
use crate::index::admin_share_group::Indexed;
use crate::support::violation;

pub(crate) fn verify_share_group_action(
    action: &ScenarioAction,
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) -> bool {
    let scenario_action = action;
    let ScenarioAction::DescribeShareGroup(action) = scenario_action else {
        return false;
    };
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
    true
}

fn description_matches(
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

fn one<T>(values: Option<&Vec<Indexed<T>>>) -> Option<&Indexed<T>> {
    let [value] = values?.as_slice() else {
        return None;
    };
    Some(value)
}
